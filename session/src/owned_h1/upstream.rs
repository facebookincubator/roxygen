/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental owned HTTP/1.1 upstream session.
//!
//! This is the poll-driven HTTP/1.1 counterpart of
//! [`crate::owned_h2::OwnedH2UpstreamSession`], with the same admission contract: reserve a slot,
//! submit a head and its body source, and read the response from a source the caller owns. One
//! driver future runs the connection's upstream `Http1xCodec`, the request upload and the
//! response source, with no task per request.
//!
//! # Capacity and reuse
//!
//! Capacity is one. It is released once the request is serialized and the response's EOM is
//! parsed. After the response's reader is done and the request's bytes are written, a
//! connection that `Http1xCodec` leaves reusable, between messages, returns its slot
//! (`capacity_available`) and is idle. Any other connection closes admission and shuts down
//! its write side. Response bytes after a finished response fail the connection before the
//! next request is committed. Peer EOF closes admission.
//!
//! A slot is returning (`OwnedH1UpstreamHandle::returning_capacity`) once its reader's
//! completion is all that keeps the driver from retiring a finished exchange on a reusable
//! connection. The connection admits the next request immediately, and the driver commits it
//! once its next turn has retired the exchange. If the slot is already taken by then, that
//! retirement does not announce capacity.
//!
//! Each turn reads before it commits a queued request, so a peer EOF or a failed read that is
//! already readable closes admission first. A request rejected because admission closed reports
//! `OwnedRequestProgressExt::request_unprocessed`.
//!
//! # Requests
//!
//! An HTTP/1.1 exchange cannot be reset alone, so a cancel before both messages end resets the
//! connection. A request body that does not match its declared `Content-Length` fails the
//! request with `CONTENT_LENGTH_MISMATCH`, and no byte past that length or after a short end is
//! written. Trailers on a `Content-Length` request cannot be encoded in HTTP/1.1 and are
//! dropped. Under the default [`Http1UpgradePolicy`], a request that can switch protocols
//! (CONNECT, an `Upgrade` header or the egress websocket flag) is rejected. Under
//! [`Http1UpgradePolicy::AsOrdinaryRequest`], CONNECT and `Upgrade` requests are sent as
//! ordinary requests, a head that does not frame its body the way the codec writes it is
//! rejected, a `101` fails the connection, and the egress websocket flag is still rejected. Byte
//! events are cancelled, never fired, and the driver has no timers.
//!
//! A cancel from any callback the driver runs, including a request body's resume future and a
//! waker run while a response event is published, takes effect at the end of that phase, before
//! the driver reads more of the request, parses the next response event, writes or closes
//! normally. As in `UniplexSession`, a failed read fails the connection only after every byte
//! read before it is parsed, flow control included, and resets the transport only once the
//! request bytes already serialized are written.
//!
//! [`OwnedH1UpstreamSession::new`] validates [`UniplexConfig`] as [`OwnedH1Session::new`] does,
//! with these differences:
//!
//! - `drain_request_body_on_drop` must be false.
//! - `unframed_bodiless_http1_requests` is supported: a request whose head ends it and that has
//!   no framing header is sent without body framing, as through `UniplexSession`.
//! - `deliver_ingress_before_abort` is supported: a response that fails keeps its received head
//!   and body events for its reader, ahead of the error. If the request's consumer cancelled it
//!   (a cancel, a dropped submission or the cancellation token), the events are discarded, as
//!   `UniplexStreamAbort` does.
//!
//! # Panic containment
//!
//! The driver contains panics from the code it runs and counts each in its worker's
//! `WorkerPollStats`. A panic in a request's upload or response observer, or in the
//! encoded-headers callback of a head with a body to follow, fails that request, which closes
//! its connection as a failed upload does. A panic in a byte-event callback, in the
//! encoded-headers callback of a head that ends its request, or in a retiring exchange is only
//! counted. A panic in the connection's observer while the driver polls closes admission, so the
//! connection closes once idle. Any other panic in the driver's poll tears the connection down:
//! its native read and writes are cancelled and joined, an operation whose teardown polls panic
//! three times in total is dropped unfinished, and the run returns `Err(INTERNAL_ERROR)`. A run
//! never resumes a panic.
//!
//! # Pull-through exchanges
//!
//! Under [`OwnedH1UpstreamSession::run_pull_through`], the body of a
//! [`OwnedH1UpstreamReservation::submit_pull_through`] exchange holds the driver's drive from its
//! first read after the request is written until the response is fully produced or its source
//! completes. While it holds the drive, the body polls the driver inline from its reads, and the
//! run's future does not poll it. `Drive` documents the wake rule this depends on.

use std::cell::OnceCell;
use std::cell::RefCell;
use std::rc::Weak;

use roxygen_core::body_event_queue::HttpBodySource;
use roxygen_core::body_event_queue::HttpBodyTransfer;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::ResumeFuture;
use roxygen_core::events::ResumeStatus;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpResponseReader;
use roxygen_core::stream_source::HttpResponseSource;
use roxygen_core::stream_source::HttpResponseSourceOwner;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
use crate::owned::OwnedResponseObserver;
use crate::owned::OwnedUpstreamObserver;

/// Connection state shared by the handle, reservations, submissions and the driver.
struct Shared {
    context: WorkerContext,
    /// The driver's exchange signal: submissions and request controls wake the driver here.
    wake: Arc<Signal>,
    complete: Cell<bool>,
    cancel: Cell<bool>,
    drain: Cell<bool>,
    idle: Cell<bool>,
    used: Cell<u32>,
    /// A committed exchange holds the slot until it retires.
    spent: Cell<bool>,
    /// The live exchange's response signals once only its consumer's completion keeps the
    /// driver from retiring it and returning the slot. From that completion on, the slot is
    /// returning, and admission counts it free.
    retiring: RefCell<Option<Rc<IngressSignals>>>,
    /// Run whenever the slot turns returning
    /// (`OwnedH1UpstreamHandle::set_returning_hook`).
    returning_hook: RefCell<Option<Rc<dyn Fn()>>>,
    /// Requests committed on the connection.
    committed: Cell<u64>,
    closed: RefCell<Option<HttpError>>,
    observer: RefCell<Option<Rc<dyn OwnedUpstreamObserver>>>,
    queued: RefCell<Option<Submission>>,
    /// The capacity release, the admission close and the response sources' callbacks wake the
    /// driver here. Its `polling` is set while the driver polls (`SharedPolling`): a callback
    /// that panics then is contained and reported, and otherwise its panic reaches the caller.
    owner: Rc<OwnerWake>,
    /// The observer panicked while the driver polled, which drains the connection.
    escalate_drain: Cell<bool>,
    /// Who polls the driver of a session run with `run_pull_through`.
    drive: Drive,
}

/// Who polls a pull-through connection's driver.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum DriveState {
    /// The run future, on each of its polls.
    #[default]
    Idle,
    /// Only the body of this exchange, from inside its reads.
    Held(StreamId),
    /// The run future: a cancel input or a connection failure took the drive from this
    /// exchange's body.
    Revoked(StreamId),
}

/// A pull-through connection's drive state. It lives in `Shared`, so that hand-back and
/// revocation reach it without borrowing the driver.
///
/// A body that holds the drive polls the driver only when the wake record of the driver's task
/// waker shows that one of the driver's wake sources fired since its last poll. So every input
/// of the driver must do one of these, or it is lost while a body holds the drive:
///
/// - wake the task waker, through a `Signal` or `task.wake()`, or make an owner-thread wake
///   (`OwnerWake::wake_local`), all of which set the record;
/// - be covered by a term of the body's check, such as a pending budget yield;
/// - reach the run's future, which takes the drive back, as the held request's cancellation
///   token and worker cancellation do.
///
/// Timers added to this driver must wake its task waker, and must take a held drive back before
/// pull-through can accept them. Taking the drive back (hand-back and revocation) touches only
/// this state, the task waker and wakers, never the driver, so it is safe from a callback inside
/// an inline drive.
#[derive(Default)]
struct Drive {
    state: Cell<DriveState>,
    /// The run future's last waker.
    run_waker: RefCell<Option<Waker>>,
    /// The holder's last waker.
    holder_waker: RefCell<Option<Waker>>,
    /// Set while a holder's read polls the driver.
    driving: Cell<bool>,
    /// The pull-through exchange whose request bytes are all written.
    written: Cell<Option<StreamId>>,
    /// A budget yield was pending when the holder's last drive returned.
    yield_pending: Cell<bool>,
    /// The held exchange's cancellation token, when it can be cancelled, registered with the
    /// run future's waker: a cancel from any thread wakes the run future, which revokes.
    token: RefCell<Option<CancellationWakeRegistration>>,
}

impl Shared {
    /// Re-registers the driver's task waker to the run future's waker, which it returns.
    fn register_run_waker(&self) -> Option<Waker> {
        let waker = self.drive.run_waker.borrow().clone();
        if let Some(waker) = &waker {
            self.wake.task.register(waker);
        }
        waker
    }

    /// Returns `stream`'s held or revoked drive to the run, before the wake that announces it:
    /// the driver's task waker is the run future's again. It does not borrow the driver.
    /// Returns whether the drive changed hands.
    fn hand_back(&self, stream: StreamId) -> bool {
        let (DriveState::Held(held) | DriveState::Revoked(held)) = self.drive.state.get() else {
            return false;
        };
        if held != stream {
            return false;
        }
        self.drive.state.set(DriveState::Idle);
        self.register_run_waker();
        drop(self.drive.token.take());
        true
    }

    /// Takes a held drive, `stream`'s or any, back to the run future, which applies what
    /// revoked it. Every cancel input and connection failure revokes. It does not borrow the
    /// driver, so it is safe from a callback inside an inline drive. It does not wake that
    /// drive's holder, whose read continues after its drive.
    fn revoke(&self, stream: Option<StreamId>) {
        let DriveState::Held(held) = self.drive.state.get() else {
            return;
        };
        if stream.is_some_and(|stream| stream != held) {
            return;
        }
        self.drive.state.set(DriveState::Revoked(held));
        let run = self.register_run_waker();
        self.wake.task.note_wake();
        drop(self.drive.token.take());
        let holder = if self.drive.driving.get() {
            None
        } else {
            self.drive.holder_waker.borrow().clone()
        };
        if let Some(run) = run {
            run.wake();
        }
        if let Some(holder) = holder {
            holder.wake();
        }
    }

    /// The connection is cancelled, by its handle or its worker.
    #[inline]
    fn cancelled(&self) -> bool {
        self.cancel.get() || self.context.is_cancelled()
    }

    /// The live exchange's consumer completed it once only that completion kept the driver from
    /// retiring it and returning the slot.
    #[inline]
    fn returning(&self) -> bool {
        self.retiring
            .borrow()
            .as_ref()
            .is_some_and(|signals| signals.source_complete.get())
    }

    /// Whether the connection rejects requests because admission closed or it was cancelled.
    #[inline]
    fn refuses(&self) -> bool {
        self.cancel.get() || self.closed.borrow().is_some() || self.context.is_cancelled()
    }

    /// 1 while the slot is free and admission is open. Capacity announcements follow it, so a
    /// returning slot is announced when the driver retires its exchange.
    fn available(&self) -> u32 {
        if self.spent.get() || self.refuses() {
            0
        } else {
            1 - self.used.get()
        }
    }

    /// `available`, counting a returning slot free: what a reservation may take.
    fn admits(&self) -> u32 {
        if (self.spent.get() && !self.returning()) || self.refuses() {
            0
        } else {
            1 - self.used.get()
        }
    }

    /// Runs one admission callback at `site`. While the driver polls, a panic is reported and
    /// disposed of, and an observer's or the returning hook's also drains the connection.
    /// Otherwise the first panic is kept in `first` for `finish_panic` to raise to the caller.
    #[inline(always)]
    fn contain_callback(
        &self,
        first: &mut Option<PanicPayload>,
        site: PanicSite,
        stream: Option<StreamId>,
        callback: impl FnOnce(),
    ) {
        if let Err(panic) = catch_unwind(AssertUnwindSafe(callback)) {
            self.callback_panicked(first, site, stream, panic);
        }
    }

    #[cold]
    #[inline(never)]
    fn callback_panicked(
        &self,
        first: &mut Option<PanicPayload>,
        site: PanicSite,
        stream: Option<StreamId>,
        panic: PanicPayload,
    ) {
        if !self.owner.polling.get() {
            if first.is_none() {
                *first = Some(panic);
            } else {
                discard_panic(panic);
            }
            return;
        }
        report_panic(&self.context, site, stream, panic);
        if site == PanicSite::UpstreamObserver {
            self.escalate_drain.set(true);
        }
    }

    fn release(&self, stream: Option<StreamId>, undelivered: Option<&Cell<bool>>) {
        self.used.set(
            self.used
                .get()
                .checked_sub(1)
                .expect("one outgoing reservation is owned"),
        );
        let observer = self.observer.borrow().clone();
        let mut panic = None;
        let site = PanicSite::UpstreamObserver;
        if let Some(observer) = observer {
            if let Some(undelivered) = undelivered {
                undelivered.set(true);
                self.contain_callback(&mut panic, site, stream, || {
                    observer.capacity_released_before_delivery()
                });
            }
            if let Some(stream) = stream {
                self.contain_callback(&mut panic, site, Some(stream), || {
                    observer.capacity_released(stream)
                });
            }
            if self.available() != 0 {
                self.contain_callback(&mut panic, site, stream, || observer.capacity_available());
            }
        }
        // The slot is returning again. Capacity callbacks do not announce that, so the
        // returning hook does.
        if self.available() == 0 && self.admits() != 0 {
            let hook = self.returning_hook.borrow().clone();
            if let Some(hook) = hook {
                self.contain_callback(&mut panic, site, stream, || hook());
            }
        }
        self.contain_callback(&mut panic, PanicSite::Transport, None, || {
            self.owner.wake_local(Some(&self.wake))
        });
        finish_panic(panic);
    }

    /// Returns the slot after a finished exchange: the connection admits a request again.
    fn return_slot(&self) {
        self.spent.set(false);
        let observer = (self.available() != 0)
            .then(|| self.observer.borrow().clone())
            .flatten();
        if let Some(observer) = observer {
            let mut panic = None;
            self.contain_callback(&mut panic, PanicSite::UpstreamObserver, None, || {
                observer.capacity_available()
            });
            finish_panic(panic);
        }
    }

    /// Closes admission with `error`: a queued submission is rejected with it.
    fn close(&self, error: HttpError) {
        let changed = {
            let mut closed = self.closed.borrow_mut();
            if closed.is_none() {
                *closed = Some(error.clone());
                true
            } else {
                false
            }
        };
        let observer = changed.then(|| self.observer.borrow().clone()).flatten();
        let queued = self.queued.borrow_mut().take();
        if !changed && queued.is_none() {
            return;
        }
        let mut panic = None;
        if let Some(mut submission) = queued {
            self.contain_callback(&mut panic, PanicSite::Submission, None, || {
                submission.refuse(error.clone())
            });
        }
        if let Some(observer) = observer {
            self.contain_callback(&mut panic, PanicSite::UpstreamObserver, None, || {
                observer.admission_closed()
            });
        }
        self.contain_callback(&mut panic, PanicSite::Transport, None, || {
            self.owner.wake_local(Some(&self.wake))
        });
        finish_panic(panic);
    }

    /// Whether an owner-thread wake during the driver's poll left work for the next poll: the
    /// exchange signal's mark, or a drive that left its holder during the holder's own drive,
    /// which only a wake announces to the run. A finished run is not woken.
    fn owner_wake_left(&self) -> bool {
        !self.complete.get()
            && (self.wake.is_ready()
                || (self.drive.driving.get()
                    && !matches!(self.drive.state.get(), DriveState::Held(_))))
    }

    /// Wakes the registered waker, the run future's or the holder's, through the task waker's
    /// lock, at the end of the poll that the wake belongs to: a waker that panics is reported.
    fn wake_after_poll(&self) {
        if let Err(panic) = guarded(|| self.wake.task.wake()) {
            report_panic(&self.context, PanicSite::Transport, None, panic);
        }
    }
}

fn finish_panic(panic: Option<PanicPayload>) {
    if let Some(panic) = panic {
        if std::thread::panicking() {
            discard_panic(panic);
        } else {
            resume_unwind(panic);
        }
    }
}

#[inline(always)]
fn guarded<T>(action: impl FnOnce() -> T) -> Result<T, PanicPayload> {
    catch_unwind(AssertUnwindSafe(action))
}

/// Reports a panic contained at `site` and disposes of it.
#[cold]
#[inline(never)]
fn report_panic(
    context: &WorkerContext,
    site: PanicSite,
    stream: Option<StreamId>,
    panic: PanicPayload,
) {
    report_contained_panic(context, site, stream, &*panic);
    discard_panic(panic);
}

/// Reports a panic in a teardown step or a released field, and disposes of it.
#[cold]
#[inline(never)]
fn teardown_panicked(context: &WorkerContext, panic: PanicPayload) {
    report_panic(context, PanicSite::Teardown, None, panic);
}

/// Reports a panic contained at an Exchange-scope `site` of the exchange `id`, disposes of it,
/// and returns the error that fails that exchange, and with it the connection.
#[cold]
#[inline(never)]
fn exchange_panicked(
    context: &WorkerContext,
    site: PanicSite,
    id: StreamId,
    panic: PanicPayload,
) -> HttpError {
    report_panic(context, site, Some(id), panic);
    failure(
        HttpErrorCode::INTERNAL_ERROR,
        b"owned H1 upstream exchange callback panicked",
    )
}

/// Drops `field` under its own unwind boundary, reporting a panic as a teardown step's.
#[inline(always)]
fn release_field<T>(context: &WorkerContext, field: T) {
    if let Err(panic) = guarded(|| drop(field)) {
        teardown_panicked(context, panic);
    }
}

/// Unwinds to the per-turn catch after a Driver-scope panic its site already reported, so the
/// catch starts the teardown without reporting it again.
struct ReportedPanic;

/// Reports a panic that reached the per-turn catch, unless its site already did.
#[cold]
#[inline(never)]
fn report_turn_panic(context: &WorkerContext, panic: &PanicPayload) {
    if !panic.is::<ReportedPanic>() {
        report_contained_panic(context, PanicSite::DriverTurn, None, &**panic);
    }
}

/// The panicking teardown polls of each native operation, counted in total.
#[derive(Default)]
struct TeardownState {
    read_panics: u8,
    write_panics: u8,
    queue_panics: u8,
}

/// The panicking teardown polls after which a native operation is dropped unfinished.
const TEARDOWN_PANIC_LIMIT: u8 = 3;

/// Marks the connection as polled by its driver while the guard lives, then restores the
/// previous mark. It holds the connection's state weakly, so it never drops the last reference
/// to it. The outermost guard's exit wakes the driver once if an owner-thread wake during the
/// poll left work for it.
struct SharedPolling(Weak<Shared>, bool);

impl SharedPolling {
    #[inline(always)]
    fn enter(shared: &Rc<Shared>) -> Self {
        Self(Rc::downgrade(shared), shared.owner.polling.replace(true))
    }
}

impl Drop for SharedPolling {
    fn drop(&mut self) {
        let Some(shared) = self.0.upgrade() else {
            return;
        };
        shared.owner.polling.set(self.1);
        if !self.1 && shared.owner.local_pending.take() && shared.owner_wake_left() {
            shared.wake_after_poll();
        }
    }
}

#[cold]
#[inline(never)]
fn poisoned_run() -> HttpError {
    failure(
        HttpErrorCode::INTERNAL_ERROR,
        b"owned H1 upstream runner unwound out of an earlier poll",
    )
}

#[cold]
#[inline(never)]
fn polled_after_completion() -> HttpError {
    failure(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H1 upstream runner polled after completion",
    )
}

/// Cancels each request byte-event registration of the exchange `id` with
/// `INVALID_STATE_TRANSITION`, as `cancel_byte_events` does, each under its own unwind boundary:
/// a panicking callback is reported, and the other registrations still get that error.
fn refuse_request_byte_events(
    context: &WorkerContext,
    id: StreamId,
    registrations: Vec<HttpByteEventRegistration>,
) {
    if registrations.is_empty() {
        return;
    }
    #[cfg(test)]
    count(&BYTE_EVENT_REFUSALS);
    let error = failure(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H1 delivers no byte events",
    );
    for mut registration in registrations {
        if let Err(panic) = guarded(|| registration.cancel(&error, None)) {
            report_panic(context, PanicSite::ByteEvent, Some(id), panic);
        }
    }
}

/// Runs the request head's encoded-headers callback with the head's encoded size, then drops
/// the head event, under one unwind boundary when it carries a callback. A panic is reported; it
/// fails the request only when the head leaves a body to follow.
fn finish_head_event(
    context: &WorkerContext,
    id: StreamId,
    mut headers: HttpHeaderEvent,
    size: Option<HttpHeaderSize>,
    eom: bool,
) -> Result<(), HttpError> {
    if !headers.has_egress_headers_callback() {
        drop(headers);
        return Ok(());
    }
    let Err(panic) = guarded(move || {
        if let Some(size) = size {
            headers.on_headers_encoded(size);
        }
        drop(headers);
    }) else {
        return Ok(());
    };
    report_panic(context, PanicSite::HeadersEncoded, Some(id), panic);
    if eom {
        return Ok(());
    }
    Err(failure(
        HttpErrorCode::INTERNAL_ERROR,
        b"owned H1 upstream exchange callback panicked",
    ))
}

/// Owner-local upstream admission and connection lifetime controls.
#[derive(Clone)]
pub struct OwnedH1UpstreamHandle(Rc<Shared>);

impl OwnedH1UpstreamHandle {
    /// 1 while the connection's slot is free or returning (`returning_capacity`) and admission
    /// is open, otherwise 0.
    pub fn available_capacity(&self) -> u32 {
        if self.0.complete.get() {
            0
        } else {
            self.0.admits()
        }
    }

    /// 1 while the slot is returning, otherwise 0.
    ///
    /// A slot is returning while it is held only by an exchange with its request written and
    /// its response complete and read to its end (its source completed), on a connection
    /// that `Http1xCodec` left reusable after that response, with admission open and no
    /// reservation taken on the slot. The slot admits the next request meanwhile. The driver's
    /// next turn, which the source's completion woke, retires the exchange and then commits a
    /// request submitted on the slot, unless the connection fails first. Once a reservation
    /// takes the slot, this reads 0 and the retirement does not announce capacity.
    pub fn returning_capacity(&self) -> u32 {
        let shared = &self.0;
        u32::from(
            shared.returning()
                && shared.used.get() == 0
                && !shared.refuses()
                && !shared.complete.get(),
        )
    }

    /// Sets a hook that runs whenever the slot becomes returning, for a pool's selection index.
    ///
    /// Observer callbacks do not announce that change. The hook runs inside the consumer's
    /// completion of a finished exchange, before the driver's next turn, and at the release of a
    /// reservation taken on the returning slot. `returning_capacity` reads 1 while it runs. A
    /// connection that closes first does not run it. The hook must not panic or act on the
    /// connection. It replaces any earlier hook.
    #[doc(hidden)]
    pub fn set_returning_hook(&self, hook: Rc<dyn Fn()>) {
        *self.0.returning_hook.borrow_mut() = Some(hook);
    }

    /// Installs the connection's one observer. Fails with `INVALID_STATE_TRANSITION` once an
    /// observer is installed or the session has finished. If admission is already closed, the
    /// observer gets `admission_closed` before this returns.
    pub fn set_observer(&self, observer: Rc<dyn OwnedUpstreamObserver>) -> Result<(), HttpError> {
        if self.0.complete.get() || self.0.observer.borrow().is_some() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned upstream observer is already installed or session finished",
            ));
        }
        *self.0.observer.borrow_mut() = Some(Rc::clone(&observer));
        if self.0.closed.borrow().is_some() {
            observer.admission_closed();
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.0.complete.get()
    }

    /// Whether the connection is idle. Idleness accounts for response consumers, upload
    /// retirement and native write ownership. The driver recomputes it on each quiescent turn,
    /// so a slot returned uncommitted leaves the connection idle again once the driver has run.
    pub fn is_idle(&self) -> bool {
        !self.is_finished() && self.0.idle.get() && self.0.used.get() == 0
    }

    /// Fails the connection with `CORO_CANCELLED`: admission closes, the live exchange is
    /// cancelled and dropped and the transport is reset, on the worker, before `run` completes.
    pub fn cancel(&self) {
        self.0.cancel.set(true);
        self.0.idle.set(false);
        self.0.revoke(None);
        self.0.wake.wake_by_ref();
    }

    /// Closes admission and closes the connection once its exchange, if any, retires.
    /// Returns whether this call changed the session's state.
    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        if self.is_finished() {
            return Ok(false);
        }
        let changed = !self.0.drain.replace(true);
        self.0.close(failure(
            HttpErrorCode::REFUSED_STREAM,
            b"owned upstream connection is draining",
        ));
        Ok(changed)
    }

    /// Takes the connection's one slot, free or returning. Fails with `REFUSED_STREAM` while the
    /// slot is taken or admission is closed.
    pub fn reserve_request(&self) -> Result<OwnedH1UpstreamReservation, HttpError> {
        if self.available_capacity() == 0 {
            return Err(failure(
                HttpErrorCode::REFUSED_STREAM,
                b"owned upstream admission is unavailable",
            ));
        }
        self.0.used.set(1);
        self.0.idle.set(false);
        Ok(OwnedH1UpstreamReservation {
            shared: Rc::clone(&self.0),
            active: true,
        })
    }

    /// Runs `inside` with the connection marked as polled by its driver, as a poll of its run
    /// does.
    #[cfg(test)]
    pub(crate) fn as_if_polled<R>(&self, inside: impl FnOnce() -> R) -> R {
        let _polling = SharedPolling::enter(&self.0);
        inside()
    }
}

/// The connection's one admission slot, returned exactly once if it is not committed.
pub struct OwnedH1UpstreamReservation {
    shared: Rc<Shared>,
    active: bool,
}

/// What releases a submission's waiter.
enum ResponseWait {
    Commitment,
    /// The first published head or the response's end, with the request producer's
    /// completion when the submitter's deadline observes it.
    FirstHead(Option<IngressCompletion>),
}

impl OwnedH1UpstreamReservation {
    /// Submits a request while preserving its fresh response read capability.
    pub fn submit_exclusive(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH1ExclusiveSubmission {
        OwnedH1ExclusiveSubmission(self.submit(headers, source, cancellation))
    }

    /// Submits a request whose response body may drive this connection from inside its reads,
    /// with `submit_exclusive`'s commitment semantics.
    ///
    /// The connection must run with [`OwnedH1UpstreamSession::run_pull_through`]. Under `run`,
    /// the driver rejects the request at commit with `INVALID_STATE_TRANSITION`, before any byte
    /// is written, and the rejection does not report `request_unprocessed`. A `cancellation`
    /// token that can be cancelled still takes effect while the body holds the drive: its cancel
    /// wakes the run's future, which takes the drive back and applies it. The request rejects
    /// `observe_response`.
    pub fn submit_pull_through(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH1PullThroughSubmission {
        OwnedH1PullThroughSubmission(self.submit_impl(
            headers,
            source,
            cancellation,
            ResponseWait::Commitment,
            true,
        ))
    }

    /// Submits a request and waits for its first published response head, preserving the
    /// fresh response read capability, as
    /// `OwnedH2UpstreamReservation::submit_with_response_interest`. Codec commitment alone does
    /// not notify this waiter. Informational heads also satisfy this interest and remain
    /// ordered in the original source for the caller to drain. A response that ends or fails
    /// before any head also releases the waiter. Upload, source and native write retirement
    /// remain independent of this notification. The caller must enforce its own timeouts while
    /// waiting, including any measured from the request producer's EOM.
    ///
    /// If the caller's deadline depends on the request producer's completion, pass that
    /// completion as `producer_completion`, unless the producer has already succeeded, since it
    /// then cannot fail. If the upload fails after the producer has failed, the request fails
    /// with the producer's error, provided that the caller has not taken the response and the
    /// producer failed before the response ended. Otherwise a failed upload fails the response
    /// with the session's upload-failure outcome. Either way the connection closes.
    pub fn submit_with_response_interest(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        producer_completion: Option<IngressCompletion>,
    ) -> OwnedH1ExclusiveSubmission {
        OwnedH1ExclusiveSubmission(self.submit_impl(
            headers,
            source,
            cancellation,
            ResponseWait::FirstHead(producer_completion),
            false,
        ))
    }

    /// Takes the already-read request head and its original body-only source.
    /// The returned future acknowledges codec commitment, not a native write.
    pub fn submit(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH1UpstreamSubmission {
        self.submit_impl(
            headers,
            source,
            cancellation,
            ResponseWait::Commitment,
            false,
        )
    }

    fn submit_impl(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        wait: ResponseWait,
        pull_through: bool,
    ) -> OwnedH1UpstreamSubmission {
        let (response_interest, producer) = match wait {
            ResponseWait::Commitment => (false, None),
            ResponseWait::FirstHead(completion) => (true, completion),
        };
        let shared = Rc::clone(&self.shared);
        let status = Rc::new(RequestStatus {
            shared: Rc::clone(&shared),
            stream: Cell::new(None),
            ingress: RefCell::new(None),
            counted: Cell::new(false),
            released: Cell::new(false),
            cancellation,
            error: OnceCell::new(),
            response: RefCell::new(None),
            response_ready: Cell::new(false),
            response_interest,
            response_published: Cell::new(false),
            delivered: Cell::new(false),
            awaiting_delivery: Cell::new(true),
            released_undelivered: Cell::new(false),
            response_observation_attached: Cell::new(false),
            response_observer: RefCell::new(None),
            live: Cell::new(false),
            waiter: RefCell::new(None),
            unprocessed: Cell::new(false),
            sequence: Cell::new(None),
            link: Cell::new(None),
            pull_through,
        });
        let cancellation_wake = status.cancellation.can_be_cancelled().then(|| {
            let mut registration = status.cancellation.wake_registration();
            registration.update_waker(&Waker::from(Arc::clone(&shared.wake)));
            registration
        });
        let mut submission = Submission {
            cancellation_wake,
            reservation: Some(self),
            headers: Some(headers),
            source: Some(source.into()),
            producer,
            status: Rc::clone(&status),
        };
        let result = OwnedH1UpstreamSubmission {
            status,
            complete: false,
        };
        let closed = shared.closed.borrow().clone();
        if let Some(error) = closed {
            submission.refuse(error);
        } else {
            *shared.queued.borrow_mut() = Some(submission);
            shared.wake.wake_by_ref();
        }
        result
    }
}

impl Drop for OwnedH1UpstreamReservation {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.shared.release(None, None);
        }
    }
}

/// A cancellation-safe acknowledgment of the driver's request commitment.
pub struct OwnedH1UpstreamSubmission {
    status: Rc<RequestStatus>,
    complete: bool,
}

impl OwnedH1UpstreamSubmission {
    fn poll_response(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedH1ExclusiveResponse, HttpError>> {
        // As OwnedH2: a cause selected before the response is claimed fails the submission,
        // whichever of it and the driver is polled first.
        if self.status.response_ready.get()
            && let Some(error) = self.status.selected_error()
        {
            self.status.fail(error);
        }
        if self.status.response_ready.get()
            && (!self.status.response_interest
                || self.status.response_published.get()
                || self.status.error.get().is_some())
        {
            let response = self
                .status
                .response
                .borrow_mut()
                .take()
                .expect("submission acknowledgment is consumed once");
            self.status.delivered.set(true);
            self.complete = true;
            self.status.retire_delivery_wait();
            return Poll::Ready(response.map(|(stream, source)| OwnedH1ExclusiveResponse {
                stream,
                source,
                control: OwnedH1UpstreamRequest(Rc::clone(&self.status)),
            }));
        }
        let previous = self
            .status
            .waiter
            .borrow_mut()
            .replace(context.waker().clone());
        drop(previous);
        Poll::Pending
    }
}

impl OwnedH1UpstreamSubmission {
    /// Retains the first explicit cause, including a caller's route timeout. Selected before
    /// the response is claimed, it fails the submission.
    pub fn cancel(&self, error: HttpError) {
        self.status.cancel(error);
    }

    #[cold]
    #[inline(never)]
    fn cancel_unresolved(&mut self) {
        let error = if self.status.response_interest && self.status.response_ready.get() {
            HttpError::new(HttpErrorCode::CANCEL)
        } else {
            failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request submission was dropped",
            )
        };
        let mut panic = None;
        capture(&mut panic, || self.status.cancel(error.clone()));
        // A ready but unclaimed raw source must not hold its own cancellation fence.
        capture(&mut panic, || self.status.fail(error));
        capture(&mut panic, || self.status.retire_delivery_wait());
        finish_panic(panic);
    }
}

/// Dropping the submission after commitment and before it resolves cancels the request with
/// `CANCEL` when it waits for response interest, otherwise with `CORO_CANCELLED`; a committed
/// HTTP/1.1 request cannot be reset alone, so the connection closes.
impl Drop for OwnedH1UpstreamSubmission {
    fn drop(&mut self) {
        if !self.complete {
            self.cancel_unresolved();
        }
    }
}

impl Future for OwnedH1UpstreamSubmission {
    type Output = Result<OwnedH1UpstreamResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_response(context).map(|result| {
            result.map(|response| OwnedH1UpstreamResponse {
                stream: response.stream,
                source: response.source.into_source_owner(),
                control: response.control,
            })
        })
    }
}

/// A submission preserving the response's fresh exclusive capability. Ordinary submissions
/// acknowledge codec commitment; response-interest submissions additionally wait for the
/// first published response head or the response's end.
pub struct OwnedH1ExclusiveSubmission(OwnedH1UpstreamSubmission);

impl OwnedH1ExclusiveSubmission {
    /// Retains the first explicit cause through the ordinary submission owner.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }
}

impl Future for OwnedH1ExclusiveSubmission {
    type Output = Result<OwnedH1ExclusiveResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_response(context)
    }
}

/// The fresh response source and the request's capacity observation.
pub struct OwnedH1ExclusiveResponse {
    pub stream: StreamId,
    pub source: HttpResponseSource,
    pub control: OwnedH1UpstreamRequest,
}

/// The original response source and a sticky capacity observation.
pub struct OwnedH1UpstreamResponse {
    pub stream: StreamId,
    pub source: HttpStreamSourceOwner,
    pub control: OwnedH1UpstreamRequest,
}

impl OwnedH1UpstreamResponse {
    /// Converts the original response to the generic source interface without reading it.
    pub fn into_source_parts(self) -> (StreamId, HttpSourceHolder, OwnedH1UpstreamRequest) {
        (self.stream, self.source.into_source_holder(), self.control)
    }
}

#[derive(Clone)]
pub struct OwnedH1UpstreamRequest(Rc<RequestStatus>);

impl OwnedH1UpstreamRequest {
    pub fn capacity_released(&self) -> bool {
        self.0.released.get()
    }

    /// Selects `error` for the request; before both messages end, the connection closes.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }

    /// Attaches one response observer, or immediately delivers an already-published result.
    /// Callbacks run outside request-state borrows. Repeated attachment is rejected, and so is
    /// observation of a pull-through exchange.
    pub fn observe_response(
        &self,
        observer: Box<dyn OwnedResponseObserver>,
    ) -> Result<(), HttpError> {
        // A pull-through exchange's deadline would be processed only on its body's reads.
        if self.0.pull_through {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation is refused on a pull-through exchange",
            ));
        }
        if self.0.response_observation_attached.get() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation is already attached",
            ));
        }
        let completed = self
            .0
            .ingress
            .borrow()
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        if let Some(completed) = completed {
            self.0.response_observation_attached.set(true);
            observer.response_complete(completed);
            return Ok(());
        }
        if !self.0.live.get() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation requires a live stream",
            ));
        }
        self.0.response_observation_attached.set(true);
        *self.0.response_observer.borrow_mut() = Some(observer);
        self.0.shared.wake.wake_by_ref();
        Ok(())
    }
}

impl OwnedRequestProgressExt for OwnedH1UpstreamSubmission {
    fn request_unprocessed(&self) -> bool {
        self.status.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.status.sequence.get()
    }
}

impl OwnedRequestProgressExt for OwnedH1ExclusiveSubmission {
    fn request_unprocessed(&self) -> bool {
        self.0.request_unprocessed()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence_number()
    }
}

impl OwnedRequestProgressExt for OwnedH1UpstreamRequest {
    fn request_unprocessed(&self) -> bool {
        self.0.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence.get()
    }
}

/// A pull-through submission: an acknowledgment of the driver's request commitment,
/// as [`OwnedH1ExclusiveSubmission`] for an ordinary submission. Dropping it before it resolves
/// cancels the request as dropping that submission does.
pub struct OwnedH1PullThroughSubmission(OwnedH1UpstreamSubmission);

impl OwnedH1PullThroughSubmission {
    /// Retains the first explicit cause through the ordinary submission owner.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }
}

impl Future for OwnedH1PullThroughSubmission {
    type Output = Result<OwnedH1PullThroughResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_response(context).map(|result| {
            result.map(|response| {
                let status = Rc::clone(&response.control.0);
                let link = status
                    .link
                    .take()
                    .expect("a committed pull-through exchange is linked");
                OwnedH1PullThroughResponse {
                    stream: response.stream,
                    body: OwnedH1PullThroughBody::new(
                        response.stream,
                        response.source,
                        link,
                        status,
                    ),
                    control: response.control,
                }
            })
        })
    }
}

impl OwnedRequestProgressExt for OwnedH1PullThroughSubmission {
    fn request_unprocessed(&self) -> bool {
        self.0.request_unprocessed()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence_number()
    }
}

/// A pull-through exchange's response: its body and the request's capacity observation.
pub struct OwnedH1PullThroughResponse {
    pub stream: StreamId,
    pub body: OwnedH1PullThroughBody,
    pub control: OwnedH1UpstreamRequest,
}

/// What a pull-through exchange's body reads through: its connection's run and its response
/// source's signals.
struct PullThroughLink {
    run: Rc<PullThrough>,
    signals: Rc<IngressSignals>,
}

/// The link from a pull-through exchange's response source to its connection's drive.
pub(super) struct DriveLink(Weak<Shared>);

impl DriveLink {
    /// Returns `stream`'s drive to the run as its source completes.
    pub(super) fn hand_back(&self, stream: StreamId) {
        if let Some(shared) = self.0.upgrade() {
            shared.hand_back(stream);
        }
    }
}

/// The waiting read a pull-through body's reader has armed or pending.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WaitingRead {
    Header,
    Body(u32),
}

/// A pull-through exchange's response body. It owns the response's exclusive reader and its
/// source owner, declared and retired in that order.
///
/// Once the request's bytes are written, the first read of a live exchange whose response is
/// still being produced takes the connection's drive, retiring any pending or armed read. It
/// waits as described below instead if it is nested in a callback of the run future's poll, or
/// if the connection or the request is already cancelled. While it holds the drive, each read
/// polls the driver with its waker if one of the driver's wake sources fired since the driver's
/// last poll, a budget yield is pending or the waker changed. It then takes the ready event
/// without waiting. If no event is ready, a read that still holds the drive leaves its waker
/// with the driver.
/// The drive returns to the run's future when the response is fully produced or the source
/// completes, and a cancel input or a connection failure revokes it. Before the drive is taken,
/// after it returns and after a revocation, reads take a ready event or wait for one with a
/// suspend-free read. Reads never return `HttpBodyEvent::suspend`.
pub struct OwnedH1PullThroughBody {
    reader: HttpResponseReader,
    owner: HttpResponseSourceOwner,
    run: Rc<PullThrough>,
    signals: Rc<IngressSignals>,
    status: Rc<RequestStatus>,
    stream: StreamId,
    /// The waker of the holder's last drive.
    holder: Option<Waker>,
    waiting: Option<WaitingRead>,
}

impl OwnedH1PullThroughBody {
    fn new(
        stream: StreamId,
        source: HttpResponseSource,
        link: PullThroughLink,
        status: Rc<RequestStatus>,
    ) -> Self {
        let (owner, mut reader) = source.split();
        reader.set_suspend_free(true);
        Self {
            reader,
            owner,
            run: link.run,
            signals: link.signals,
            status,
            stream,
            holder: None,
            waiting: None,
        }
    }

    /// Reads the next response head, 1xx heads included, as the response's reader does.
    pub fn poll_header(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if self.holds() || self.acquire() {
            self.drive_if_woken(context);
            if let Some(result) = self.reader.try_read_header(&self.owner) {
                return Poll::Ready(result);
            }
            if self.holds() {
                return Poll::Pending;
            }
        }
        self.wait_header(context)
    }

    /// Reads the next body event, of at most `maximum` body bytes. It never returns
    /// `HttpBodyEvent::suspend`.
    #[inline]
    pub fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if self.holds() || self.acquire() {
            self.drive_if_woken(context);
            if let Some(result) = self.reader.try_read_body(&self.owner, maximum) {
                return Poll::Ready(result);
            }
            // While the body holds the drive, its waker is the driver's.
            if self.holds() {
                return Poll::Pending;
            }
        }
        self.wait_body(context, maximum)
    }

    /// Stops the response: the reader's read retires before the source is cleared with `error`,
    /// as the reader and owner of a plain exclusive response are retired, then the drive
    /// returns to the run, if the source's completion did not already return it.
    pub fn stop(&mut self, error: Option<HttpErrorCode>) {
        self.reader.retire_active_read();
        self.waiting = None;
        self.owner.clear(error);
        let shared = &self.run.shared;
        if shared.hand_back(self.stream) {
            shared.wake.wake_by_ref();
        }
    }

    /// Whether this body holds its connection's drive.
    #[cfg(test)]
    pub(crate) fn holds_the_drive(&self) -> bool {
        self.holds()
    }

    #[inline]
    fn holds(&self) -> bool {
        self.run.shared.drive.state.get() == DriveState::Held(self.stream)
    }

    /// Takes the drive once this exchange is live, its request written, its response still
    /// being produced (so the release that returns the drive has not run) and its source
    /// incomplete: the reader's pending or armed read retires, and the next drive is
    /// unconditional. A read nested in a callback of the run future's poll does not take it:
    /// it could not drive, and the driver's wakes would reach only the run future. Nor does a
    /// read after a connection cancel or a request's error: that cancel's revocation ran while
    /// the drive was idle, so the run future must apply it.
    fn acquire(&mut self) -> bool {
        let drive = &self.run.shared.drive;
        if drive.state.get() != DriveState::Idle
            || !self.status.live.get()
            || drive.written.get() != Some(self.stream)
            || self.signals.source_complete.get()
            || self
                .status
                .ingress
                .borrow()
                .as_ref()
                .is_none_or(|ingress| ingress.snapshot().is_some())
            || self.run.driver.try_borrow().is_err()
            || self.run.shared.cancelled()
            || self.status.error.get().is_some()
        {
            return false;
        }
        // A cancellable token is registered with the run future's waker before it is checked,
        // so a cancel from another thread is not missed.
        let cancellation = &self.status.cancellation;
        let token = cancellation.can_be_cancelled().then(|| {
            let mut registration = cancellation.wake_registration();
            if let Some(run) = drive.run_waker.borrow().as_ref() {
                registration.update_waker(run);
            }
            registration
        });
        if cancellation.is_cancellation_requested() {
            return false;
        }
        self.reader.retire_active_read();
        self.waiting = None;
        drop(drive.token.replace(token));
        drive.state.set(DriveState::Held(self.stream));
        self.holder = None;
        true
    }

    /// Polls the driver if one of its wake sources fired since its last poll, a budget yield
    /// is pending, or the holder's waker changed.
    #[inline]
    fn drive_if_woken(&mut self, context: &mut Context<'_>) {
        if !self.run.task.woken()
            && !self.run.shared.drive.yield_pending.get()
            && self
                .holder
                .as_ref()
                .is_some_and(|holder| holder.will_wake(context.waker()))
        {
            return;
        }
        if !self
            .holder
            .as_ref()
            .is_some_and(|holder| holder.will_wake(context.waker()))
        {
            self.holder = Some(context.waker().clone());
            let previous = self
                .run
                .shared
                .drive
                .holder_waker
                .replace(Some(context.waker().clone()));
            drop(previous);
        }
        self.run.drive(context);
    }

    /// Whether the reader holds the waiting read `read`; another armed or pending read retires.
    fn holds_waiting(&mut self, read: WaitingRead) -> bool {
        if !self.reader.is_active() {
            return false;
        }
        if self.waiting == Some(read) {
            return true;
        }
        self.reader.retire_active_read();
        false
    }

    /// Reads without the drive: a ready event first, else a suspend-free waiting read that the
    /// producer wakes.
    fn wait_header(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if !self.holds_waiting(WaitingRead::Header) {
            if let Some(result) = self.reader.try_read_header(&self.owner) {
                return Poll::Ready(result);
            }
            if let Err(error) = self.reader.arm_header(&self.owner) {
                return Poll::Ready(Err(error));
            }
            self.waiting = Some(WaitingRead::Header);
        }
        let poll = self.reader.poll_header(context);
        if !self.reader.is_active() {
            self.waiting = None;
        }
        poll
    }

    fn wait_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if !self.holds_waiting(WaitingRead::Body(maximum)) {
            if let Some(result) = self.reader.try_read_body(&self.owner, maximum) {
                return Poll::Ready(result);
            }
            if let Err(error) = self.reader.arm_body(&self.owner, maximum) {
                return Poll::Ready(Err(error));
            }
            self.waiting = Some(WaitingRead::Body(maximum));
        }
        let poll = self.reader.poll_body(context);
        if !self.reader.is_active() {
            self.waiting = None;
        }
        poll
    }
}

impl Drop for OwnedH1PullThroughBody {
    fn drop(&mut self) {
        self.stop(None);
    }
}

struct RequestStatus {
    shared: Rc<Shared>,
    stream: Cell<Option<StreamId>>,
    ingress: RefCell<Option<IngressCompletion>>,
    counted: Cell<bool>,
    released: Cell<bool>,
    cancellation: CancellationToken,
    error: OnceCell<HttpError>,
    response: RefCell<Option<Result<(StreamId, HttpResponseSource), HttpError>>>,
    response_ready: Cell<bool>,
    response_interest: bool,
    response_published: Cell<bool>,
    delivered: Cell<bool>,
    awaiting_delivery: Cell<bool>,
    released_undelivered: Cell<bool>,
    response_observation_attached: Cell<bool>,
    response_observer: RefCell<Option<Box<dyn OwnedResponseObserver>>>,
    /// The exchange is live in the driver, which polls an attached observer.
    live: Cell<bool>,
    waiter: RefCell<Option<Waker>>,
    /// Its connection rejected it before commit.
    unprocessed: Cell<bool>,
    /// The connection's earlier commits, fixed at commit or at that rejection.
    sequence: Cell<Option<u64>>,
    /// A committed pull-through exchange's link, until its submission resolves.
    link: Cell<Option<PullThroughLink>>,
    /// Submitted with `submit_pull_through`.
    pull_through: bool,
}

impl RequestStatus {
    fn ingress_succeeded(&self) -> bool {
        self.ingress
            .borrow()
            .as_ref()
            .is_some_and(|receipt| matches!(receipt.snapshot(), Some(Ok(_))))
    }

    fn selected_error(&self) -> Option<HttpError> {
        if let Some(error) = self.error.get() {
            return Some(error.clone());
        }
        self.cancellation.is_cancellation_requested().then(|| {
            failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request body production cancelled",
            )
        })
    }

    fn cancel(&self, error: HttpError) {
        drop(self.error.set(error));
        if let Some(stream) = self.stream.get() {
            self.shared.revoke(Some(stream));
        }
        self.shared.wake.wake_by_ref();
    }

    /// Whether a cause is selected for the request: its consumer's cancel (the request's or its
    /// submission's cancel, the submission's drop, or its cancellation token), or a supplied
    /// producer's failure, which is selected only before the response is delivered.
    fn cause_selected(&self) -> bool {
        self.error.get().is_some() || self.cancellation.is_cancellation_requested()
    }

    fn notify(&self) {
        let waiter = self.waiter.borrow_mut().take();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn fail(&self, error: HttpError) {
        drop(self.error.set(error));
        let error = self
            .error
            .get()
            .expect("the request retains its first error")
            .clone();
        if !self.delivered.get() {
            let old = self.response.borrow_mut().replace(Err(error));
            self.response_ready.set(true);
            let mut panic = None;
            capture(&mut panic, || drop(old));
            capture(&mut panic, || self.notify());
            finish_panic(panic);
        }
    }

    fn acknowledge(&self, stream: StreamId, source: HttpResponseSource) {
        assert!(!self.response_ready.get(), "request is acknowledged once");
        *self.response.borrow_mut() = Some(Ok((stream, source)));
        self.response_ready.set(true);
        if !self.response_interest || self.response_published.get() {
            self.notify();
        }
    }

    /// Releases a response-interest waiter once its source holds a first head or a terminal
    /// outcome, which the caller then reads from the source.
    fn publish_response(&self) {
        if self.response_interest
            && !self.response_published.replace(true)
            && self.response_ready.get()
        {
            self.notify();
        }
    }

    fn release(&self) {
        if self.counted.replace(false) {
            self.released.set(true);
            let undelivered = self
                .awaiting_delivery
                .get()
                .then_some(&self.released_undelivered);
            self.shared.release(self.stream.get(), undelivered);
        }
    }

    fn retire_delivery_wait(&self) {
        self.awaiting_delivery.set(false);
        if self.released_undelivered.replace(false) {
            let observer = self.shared.observer.borrow().clone();
            if let Some(observer) = observer {
                observer.released_submission_retired();
            }
        }
    }
}

struct Submission {
    cancellation_wake: Option<CancellationWakeRegistration>,
    reservation: Option<OwnedH1UpstreamReservation>,
    headers: Option<HttpHeaderEvent>,
    source: Option<HttpBodySource>,
    producer: Option<IngressCompletion>,
    status: Rc<RequestStatus>,
}

impl Submission {
    /// Rejects the submission because its connection's admission closed before commit. Unless
    /// its consumer selected a cause, it failed unprocessed. One read of the selected cause
    /// determines both the flag and the error, because a cancellation can arrive from another
    /// thread.
    fn refuse(&mut self, error: HttpError) {
        let selected = self.status.selected_error();
        #[cfg(test)]
        crate::experimental::refusal_cause_sampled();
        let error = selected.unwrap_or_else(|| {
            self.status.unprocessed.set(true);
            self.status
                .sequence
                .set(Some(self.status.shared.committed.get()));
            error
        });
        self.reject_selected(error);
    }

    fn reject(&mut self, error: HttpError) {
        let error = self.status.selected_error().unwrap_or(error);
        self.reject_selected(error);
    }

    fn reject_selected(&mut self, error: HttpError) {
        let mut panic = None;
        capture(&mut panic, || self.status.fail(error.clone()));
        if let Some(headers) = self.headers.as_mut() {
            for registration in headers.byte_event_registrations_mut() {
                capture(&mut panic, || registration.cancel(&error, None));
            }
        }
        capture(&mut panic, || drop(self.headers.take()));
        if let Some(source) = self.source.as_mut() {
            capture(&mut panic, || source.clear(Some(error.code())));
        }
        capture(&mut panic, || drop(self.source.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.reservation.take()));
        finish_panic(panic);
    }
}

impl Drop for Submission {
    fn drop(&mut self) {
        if self.reservation.is_some() {
            self.reject(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request handoff retired before commitment",
            ));
        }
    }
}

/// The one committed exchange: its upload and its response producer.
struct Exchange {
    status: Rc<RequestStatus>,
    id: StreamId,
    signals: Rc<IngressSignals>,
    producer: HttpStreamSourceProducer,
    transfer: Option<HttpBodyTransfer>,
    unused_source: Option<HttpBodySource>,
    /// A suspended upload's resume, polled before the next body read.
    resume: Option<ResumeFuture>,
    /// Wakes the driver when the request's cancellation token is cancelled.
    cancellation_wake: Option<CancellationWakeRegistration>,
    response_observer: Option<Box<dyn OwnedResponseObserver>>,
    /// The supplied request producer's completion while its failure can still be selected.
    request_producer: Option<IngressCompletion>,
    /// The request's declared `Content-Length`, which its body must match.
    expected_request_length: Option<u64>,
    /// Request body bytes serialized so far.
    request_body_length: u64,
    /// The request's end is serialized.
    request_complete: bool,
    /// The response's end was parsed.
    response_complete: bool,
    /// The caller's response source completed.
    consumed: bool,
    write_boundary: u64,
}

impl Exchange {
    /// Whether `cancel_error` selects a cause, without building it. Its terms are pure, so
    /// their order sets only the cost: the worker's flag, one pointer further away, comes last.
    #[inline]
    fn cancel_requested(&self) -> bool {
        let shared = &self.status.shared;
        shared.cancel.get()
            || (!(self.request_complete && self.response_complete)
                && (self.status.error.get().is_some()
                    || self.status.cancellation.is_cancellation_requested()))
            || shared.context.is_cancelled()
    }

    /// What stops the exchange now: a connection cancel, or the request's selected cause
    /// while it still closes the connection (an HTTP/1.1 exchange cannot be reset alone before
    /// both messages end). The cause is selected only once `cancel_requested` holds.
    #[inline]
    fn cancel_error(&self) -> Option<HttpError> {
        if self.cancel_requested() {
            self.select_cancel_error()
        } else {
            None
        }
    }

    #[cold]
    #[inline(never)]
    fn select_cancel_error(&self) -> Option<HttpError> {
        #[cfg(test)]
        count(&CANCEL_SELECTIONS);
        if self.status.shared.cancelled() {
            return Some(cancelled());
        }
        if self.request_complete && self.response_complete {
            return None;
        }
        self.status.selected_error()
    }

    /// As OwnedH2: the sticky producer result takes precedence over the observer's deadline.
    fn poll_response_observer(&mut self, context: &mut Context<'_>) {
        if self.response_observer.is_none() {
            self.response_observer = self.status.response_observer.borrow_mut().take();
        }
        let Some(observer) = self.response_observer.as_mut() else {
            self.retire_producer_watch();
            return;
        };
        let completed = self
            .status
            .ingress
            .borrow()
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        let completed = match completed {
            Some(completed) => completed,
            None => match observer.poll_deadline(context) {
                Poll::Ready(error) => Err(error),
                Poll::Pending => return,
            },
        };
        self.response_observer
            .take()
            .expect("a polled response observer is owned")
            .response_complete(completed);
    }

    fn retire_response_observer(&mut self) {
        let observer = self
            .response_observer
            .take()
            .or_else(|| self.status.response_observer.borrow_mut().take());
        if let Some(observer) = observer {
            let completed = self
                .status
                .ingress
                .borrow()
                .as_ref()
                .and_then(IngressCompletion::snapshot);
            if let Some(completed) = completed {
                observer.response_complete(completed);
            }
        }
    }

    /// Reads at most `maximum` request body bytes, after any suspension resumes, as
    /// `OwnedH2UpstreamSession` polls its upload.
    fn poll_upload(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Result<Option<HttpBodyEvent>, HttpError> {
        if let Some(resume) = &mut self.resume {
            match resume.as_mut().poll(context) {
                Poll::Pending => return Ok(None),
                Poll::Ready(ResumeStatus::Cancelled) => return Err(resume_cancelled()),
                Poll::Ready(_) => {
                    self.resume = None;
                }
            }
            // The resume future is caller code: it may cancel the connection or the request.
            if self.cancel_error().is_some() {
                return Ok(None);
            }
        }
        let transfer = self
            .transfer
            .as_mut()
            .expect("an unfinished upload retains its transfer");
        match transfer.poll_resume(context) {
            Poll::Pending => return Ok(None),
            Poll::Ready(Some(ResumeStatus::Cancelled)) => return Err(resume_cancelled()),
            Poll::Ready(Some(_)) if self.status.selected_error().is_some() => return Ok(None),
            Poll::Ready(_) => {}
        }
        let Poll::Ready(result) =
            transfer.poll_read_body_event(context, maximum, &self.status.cancellation)
        else {
            return Ok(None);
        };
        result.map(Some)
    }

    /// Stops watching the supplied producer once it has succeeded, which is terminal, or once
    /// the response has ended while it has not failed.
    fn retire_producer_watch(&mut self) {
        let Some(producer) = &self.request_producer else {
            return;
        };
        let retire = match producer.snapshot() {
            Some(Ok(_)) => true,
            Some(Err(_)) => false,
            None => self
                .status
                .ingress
                .borrow()
                .as_ref()
                .is_some_and(|ingress| ingress.snapshot().is_some()),
        };
        if retire {
            self.request_producer = None;
        }
    }

    /// Selects the supplied request producer's failure for an upload that failed before the
    /// submitter took the response and before the response ended.
    fn select_producer_failure(&self) -> Option<HttpError> {
        if self.status.delivered.get() {
            return None;
        }
        let Some(Err(error)) = self
            .request_producer
            .as_ref()
            .and_then(IngressCompletion::snapshot)
        else {
            return None;
        };
        drop(self.status.error.set(error));
        Some(
            self.status
                .error
                .get()
                .expect("the request retains its first error")
                .clone(),
        )
    }

    /// The connection's error for a failed upload: the selected producer failure, or the
    /// upload's own error with the response failed as the session's upload-failure outcome.
    #[cold]
    fn upload_failed(&self, error: HttpError) -> HttpError {
        if let Some(selected) = self.select_producer_failure() {
            return selected;
        }
        self.producer.abort_with_error(
            failure(HttpErrorCode::CORO_CANCELLED, b"egress source failed"),
            b"egress source failed",
        );
        error
    }

    /// Fails the response with `error` and stops the upload. A cancel the request's consumer
    /// selected discards the response's queued events, as `UniplexStreamAbort` does.
    fn cancel(&mut self, error: &HttpError) {
        let retirement = self
            .transfer
            .as_mut()
            .map(|transfer| transfer.prepare_stop_and_clear(error.clone()));
        // A selected cause is the consumer's cancel, or a producer failure whose response is
        // then dropped undelivered: neither keeps the response's queued events.
        let discard = self.status.cause_selected();
        let mut panic = None;
        capture(&mut panic, || {
            let details = b"owned H1 upstream exchange cancelled";
            if discard {
                self.producer
                    .abort_discarding_queued(error.clone(), details)
            } else {
                self.producer.abort_with_error(error.clone(), details)
            }
        });
        if !self.status.response_ready.get() || self.status.selected_error().is_some() {
            capture(&mut panic, || self.status.fail(error.clone()));
        } else {
            capture(&mut panic, || self.status.publish_response());
        }
        if let Some(source) = self.unused_source.as_mut() {
            capture(&mut panic, || source.clear(Some(error.code())));
        }
        capture(&mut panic, || drop(self.unused_source.take()));
        if let Some(retirement) = retirement
            && let Err(payload) = retirement.retire()
        {
            if panic.is_none() {
                panic = Some(payload);
            } else {
                discard_panic(payload);
            }
        }
        if let Some(transfer) = self.transfer.as_mut() {
            capture(&mut panic, || transfer.drop_pending_read());
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        self.request_complete = true;
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        let mut panic = None;
        if !self.request_complete {
            capture(&mut panic, || {
                self.cancel(&failure(
                    HttpErrorCode::CORO_CANCELLED,
                    b"upstream upload owner retired",
                ))
            });
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        capture(&mut panic, || drop(self.unused_source.take()));
        self.status.live.set(false);
        capture(&mut panic, || self.status.release());
        capture(&mut panic, || self.status.publish_response());
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }
}

#[cold]
#[inline(never)]
fn request_length_mismatch() -> HttpError {
    failure(
        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        b"request Content-Length did not match body bytes",
    )
}

#[cold]
#[inline(never)]
fn pull_through_requires_its_run() -> HttpError {
    failure(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H1 pull-through submission requires a session run with run_pull_through",
    )
}

#[cold]
#[inline(never)]
fn resume_cancelled() -> HttpError {
    failure(
        HttpErrorCode::CORO_CANCELLED,
        b"request body resume cancelled",
    )
}

/// A poll-driven HTTP/1.1 upstream connection with one exchange at a time.
pub struct OwnedH1UpstreamSession(Option<UpstreamDriver>);

/// A session dropped unrun is abandoned as a run would be.
impl Drop for OwnedH1UpstreamSession {
    fn drop(&mut self) {
        if let Some(driver) = self.0.take() {
            driver.abandon(false);
        }
    }
}

impl OwnedH1UpstreamSession {
    pub fn new(options: OwnedH1Options) -> Result<(Self, OwnedH1UpstreamHandle), HttpError> {
        let (driver, handle) = UpstreamDriver::new(options)?;
        Ok((Self(Some(driver)), handle))
    }

    /// Runs the session inside the caller's registered worker cleanup scope (see
    /// `WorkerContext::register_scope`), as `OwnedH2Session::run` does.
    ///
    /// Dropping the run after its first poll and before it completes panics, outside a forced
    /// worker teardown, because its native I/O has not joined. To stop early, call the handle's
    /// `cancel` and poll the run to completion. Dropping the session or its run before the first
    /// poll does not panic.
    pub fn run(mut self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        UpstreamRun {
            driver: self.0.take(),
            poisoned: false,
        }
    }

    /// Runs the session like [`Self::run`], for a connection whose
    /// [`OwnedH1UpstreamReservation::submit_pull_through`] exchanges may hold its drive.
    ///
    /// It is rooted as `run` is, and behaves as `run` while no pull-through exchange holds the
    /// drive. Plain submissions behave as under `run`. While a body holds the drive, this future
    /// does not poll the driver. A cancel input, a connection failure or this future's drop takes
    /// the drive back, after which it polls the driver immediately. If the run completes inside a
    /// body's read, this future marks the run finished, releases the driver and returns the
    /// result.
    ///
    /// Worker cancellation is not one of the driver's wake sources, as under `run`. The future's
    /// root must therefore poll it at worker quiescence, and poll or drop it at worker
    /// cancellation, as the owned client's run scope does. Otherwise a held drive is taken back
    /// only at the body's next read.
    pub fn run_pull_through(
        mut self,
    ) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        let mut driver = self.0.take().expect("an unrun session holds its driver");
        let shared = Rc::clone(&driver.shared);
        let task = Arc::clone(&driver.task);
        PullThroughRun(Rc::new_cyclic(|run| {
            driver.pull_through = Weak::clone(run);
            PullThrough {
                shared,
                task,
                driver: RefCell::new(Some(driver)),
                outcome: RefCell::new(None),
                poisoned: Cell::new(false),
            }
        }))
    }
}

/// A pull-through connection's driver, and the result an inline poll reached, which only the
/// run future takes.
struct PullThrough {
    shared: Rc<Shared>,
    task: Arc<TaskWaker>,
    driver: RefCell<Option<UpstreamDriver>>,
    outcome: RefCell<Option<Result<UniplexRunSummary, HttpError>>>,
    /// Set while a poll of the driver runs and cleared as it returns, as `UpstreamRun`'s is.
    poisoned: Cell<bool>,
}

impl PullThrough {
    /// Polls the driver from the holder's read, with its context; a read nested in a callback
    /// of that poll skips. A completion is stashed for the run future, which it wakes: only the
    /// run future marks the run finished and releases the driver.
    fn drive(&self, context: &mut Context<'_>) {
        let Ok(mut cell) = self.driver.try_borrow_mut() else {
            return;
        };
        let Some(driver) = cell.as_mut() else {
            return;
        };
        if self.poisoned.get() {
            return;
        }
        let drive = &self.shared.drive;
        self.poisoned.set(true);
        drive.driving.set(true);
        let poll = {
            let _polling = SharedPolling::enter(&driver.shared);
            driver.poll_contained(context)
        };
        drive.driving.set(false);
        drive.yield_pending.set(driver.budget_yield.is_some());
        self.poisoned.set(false);
        let Poll::Ready(result) = poll else {
            return;
        };
        drop(cell);
        *self.outcome.borrow_mut() = Some(result);
        let run = drive.run_waker.borrow().clone();
        if let Some(run) = run {
            run.wake();
        }
    }
}

/// The future `run_pull_through` returns: it polls the driver unless a body holds the drive,
/// and joins and drops it.
struct PullThroughRun(Rc<PullThrough>);

impl Future for PullThroughRun {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let run = &self.0;
        if run.poisoned.get() {
            return Poll::Ready(Err(poisoned_run()));
        }
        let drive = &run.shared.drive;
        let previous = {
            let mut waker = drive.run_waker.borrow_mut();
            if waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
            {
                None
            } else {
                waker.replace(context.waker().clone())
            }
        };
        drop(previous);
        let outcome = run.outcome.borrow_mut().take();
        if let Some(result) = outcome {
            let driver = run
                .driver
                .borrow_mut()
                .take()
                .expect("a stashed completion leaves its driver to the run");
            let _polling = SharedPolling::enter(&run.shared);
            return Poll::Ready(driver.finish_run(result));
        }
        // While a body holds the drive, the run future revokes for the inputs that reach it
        // instead of the holder: the held exchange's token and worker cancellation.
        if let DriveState::Held(_) = drive.state.get() {
            let token_cancelled = drive
                .token
                .borrow_mut()
                .as_mut()
                .is_some_and(|token| token.update_waker(context.waker()));
            if !token_cancelled && !run.shared.context.is_cancelled() {
                return Poll::Pending;
            }
            run.shared.revoke(None);
        }
        let mut cell = run.driver.borrow_mut();
        let Some(driver) = cell.as_mut() else {
            return Poll::Ready(Err(polled_after_completion()));
        };
        run.poisoned.set(true);
        let polling = SharedPolling::enter(&driver.shared);
        let polled = match driver.poll_contained(context) {
            Poll::Pending => {
                drop(cell);
                Poll::Pending
            }
            Poll::Ready(result) => {
                let driver = cell
                    .take()
                    .expect("owned H1 upstream runner completes once");
                drop(cell);
                Poll::Ready(driver.finish_run(result))
            }
        };
        drop(polling);
        #[cfg(test)]
        fault_upstream_run_poll(&polled);
        run.poisoned.set(false);
        polled
    }
}

impl Drop for PullThroughRun {
    fn drop(&mut self) {
        let run = &self.0;
        if let Err(panic) = guarded(|| run.shared.revoke(None)) {
            report_panic(&run.shared.context, PanicSite::Transport, None, panic);
        }
        let outcome = run.outcome.borrow_mut().take();
        let Some(driver) = run.driver.borrow_mut().take() else {
            return;
        };
        let Some(result) = outcome else {
            driver.abandon(run.poisoned.get());
            return;
        };
        // A stashed completion proves the cleanup joined: the driver finishes as a completed
        // run's does.
        let _polling = SharedPolling::enter(&run.shared);
        drop(driver.finish_run(result));
    }
}

/// Makes the poll of a run panic outside the driver's boundaries once a test has armed it.
#[cfg(test)]
fn fault_upstream_run_poll(polled: &Poll<Result<UniplexRunSummary, HttpError>>) {
    if polled.is_pending() && UPSTREAM_RUN_FAULT.take() {
        panic!("owned H1 upstream run fault");
    }
}

/// Drives an [`UpstreamDriver`] to completion. Polls never unwind out of driver code: a
/// panic that reaches the per-turn boundary is reported and starts a teardown that joins native
/// I/O, after which the run returns `Err`. Completion publishes `complete` and releases the
/// driver before returning. A run dropped unfinished is abandoned.
struct UpstreamRun {
    driver: Option<UpstreamDriver>,
    /// Set while a poll runs and cleared as it returns, so it stays set only after a poll that
    /// unwound. Such a run returns `Err` without touching its driver again, and its abandonment
    /// does not assert that its cleanup joined.
    poisoned: bool,
}

impl Drop for UpstreamRun {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abandon(self.poisoned);
        }
    }
}

impl Future for UpstreamRun {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.poisoned {
            return Poll::Ready(Err(poisoned_run()));
        }
        let Some(driver) = this.driver.as_mut() else {
            return Poll::Ready(Err(polled_after_completion()));
        };
        this.poisoned = true;
        let polled = {
            let _polling = SharedPolling::enter(&driver.shared);
            driver.poll_contained(context).map(|result| {
                this.driver
                    .take()
                    .expect("owned H1 upstream runner completes once")
                    .finish_run(result)
            })
        };
        #[cfg(test)]
        fault_upstream_run_poll(&polled);
        this.poisoned = false;
        polled
    }
}

struct UpstreamDriver {
    context: WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    shared: Rc<Shared>,
    config: UniplexConfig,
    codec: IngressCodec,
    exchange: Option<Box<Exchange>>,
    ingress: BufQueue,
    egress: BufQueue,
    read: ReadSlot,
    write: Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<Box<dyn UniplexWriteQueue>>,
    task: Arc<TaskWaker>,
    read_wake: Arc<Signal>,
    read_waker: Waker,
    write_wake: Arc<Signal>,
    write_waker: Waker,
    stream_wake: Arc<Signal>,
    stream_waker: Waker,
    cancellation: CancellationSource,
    write_cancellation: CancellationSource,
    eof: bool,
    eof_processed: bool,
    /// A failed read, applied once the bytes read before it are parsed.
    read_error: Option<HttpError>,
    /// An applied failed read, which is the run's error. The connection resets once the
    /// request bytes already serialized are written.
    failed_read: Option<HttpError>,
    /// Parsing waits while the unread response body is at `ingress_buffer_limit`.
    flow_pause: bool,
    /// The response ended. Parsing stays paused until the next request is committed.
    response_pause: bool,
    /// The request body was not polled because the next write must make room for it.
    body_awaits_egress: bool,
    fatal: Option<HttpError>,
    transport_reset: bool,
    write_end: u64,
    summary: UniplexRunSummary,
    budget_yield: Option<LocalFuture>,
    started: bool,
    /// `Http1UpgradePolicy::AsOrdinaryRequest`: CONNECT and `Upgrade` requests are sent as
    /// ordinary requests.
    ordinary_upgrades: bool,
    /// The run of a session run with `run_pull_through`.
    pull_through: Weak<PullThrough>,
    /// Set once a Driver-scope panic started the teardown.
    teardown: Option<Box<TeardownState>>,
}

/// What a commitment leaves to run outside its unwind boundary, because it runs caller code:
/// the request head's callbacks and the submitter's acknowledgment.
struct Committed {
    headers: HttpHeaderEvent,
    header_size: Option<HttpHeaderSize>,
    eom: bool,
    status: Rc<RequestStatus>,
    source: HttpResponseSource,
    id: StreamId,
}

/// The codec callback: borrows what one response `on_ingress_with_callback` call may change.
struct ResponseInput<'a> {
    config: &'a UniplexConfig,
    exchange: &'a mut Option<Box<Exchange>>,
    stream_wake: &'a Arc<Signal>,
    flow_pause: &'a mut bool,
    response_pause: &'a mut bool,
    failure: Option<HttpError>,
}

impl ResponseInput<'_> {
    fn fail(&mut self, error: HttpError) {
        self.failure.get_or_insert(error);
    }

    /// Handles one response event. Out of line, so that the chunk boundaries `on_ingress_event`
    /// skips do not set up this frame.
    #[inline(never)]
    fn handle_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        #[cfg(test)]
        count(&RESPONSE_EVENTS);
        let mut actions = Vec::new();
        if self.failure.is_some() {
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        let Some(exchange) = self.exchange.as_mut() else {
            self.fail(failure(
                HttpErrorCode::PROTOCOL_ERROR,
                b"owned H1 upstream response without a request",
            ));
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        };
        // Publishing an event can wake code that cancels: the next event is not parsed for it.
        if let Some(error) = exchange.cancel_error() {
            self.fail(error);
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        match event {
            CodecEvent::HeadersComplete { message, .. } => {
                if !exchange.producer.headers_boxed(message, false) {
                    self.fail(failure(
                        HttpErrorCode::HEADER_PARSE_ERROR,
                        b"upstream source rejected response headers",
                    ));
                } else {
                    exchange.status.publish_response();
                }
                self.stream_wake.mark();
            }
            CodecEvent::Body { body, padding, .. } => {
                match exchange.producer.body(body, padding, false) {
                    FlowControlState::Error => self.fail(failure(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"upstream source rejected body bytes",
                    )),
                    state
                        if state == FlowControlState::Closed
                            || exchange.producer.body_bytes_buffered().is_some_and(
                                |buffered| buffered >= self.config.ingress_buffer_limit,
                            ) =>
                    {
                        *self.flow_pause = true;
                        actions.push(CodecCallbackAction::SetParserPaused(true));
                    }
                    _ => {}
                }
            }
            CodecEvent::TrailersComplete { trailers, .. } => {
                // As OwnedH2: the source's `trailers` leaves the body open for a later EOM, and
                // the trailers end this message, so EOM follows them here.
                exchange.response_complete = true;
                if !(exchange.producer.trailers(*trailers) && exchange.producer.eom()) {
                    self.fail(failure(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"upstream source rejected trailers",
                    ));
                }
                self.stream_wake.mark();
            }
            CodecEvent::MessageComplete { .. } => {
                if !exchange.response_complete {
                    exchange.response_complete = true;
                    // A rejected EOM leaves the response source terminal, and the caller reads
                    // that error from the source.
                    if !exchange.producer.eom() {
                        exchange.status.publish_response();
                    }
                }
                self.stream_wake.mark();
                *self.response_pause = true;
                actions.push(CodecCallbackAction::SetParserPaused(true));
            }
            CodecEvent::Error { error, .. } => {
                let code = http_exception_to_http_error_code(error);
                self.fail(HttpError::with_message(code, error.message()));
            }
            _ => {}
        }
        actions.into()
    }
}

/// Counts one response codec callback, and whether it came through `dyn HttpCodecCallback`: a
/// `#[track_caller]` method called through a trait object reports its own definition as its
/// caller, and a direct call reports the call site, in the codec.
#[cfg(test)]
fn count_response_delivery(caller: &std::panic::Location<'_>) {
    count(&RESPONSE_DELIVERIES);
    if caller.file() == file!() {
        count(&DYN_RESPONSE_DELIVERIES);
    }
}

impl HttpCodecCallback for ResponseInput<'_> {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn ingress_body_limit(&self, _stream: StreamId) -> Option<usize> {
        ingress_body_limit(&self.exchange.as_ref()?.producer, self.config)
    }

    #[cfg_attr(test, track_caller)]
    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        #[cfg(test)]
        count_response_delivery(std::panic::Location::caller());
        // The response ignores chunk boundaries: one acts only to stop the parse, for a failure,
        // a missing exchange or a cancel, so a boundary without any of them returns immediately.
        if let CodecEvent::ChunkHeader {
            stream: _,
            length: _,
        }
        | CodecEvent::ChunkComplete { stream: _ } = event
            && self.failure.is_none()
            && self
                .exchange
                .as_ref()
                .is_some_and(|exchange| !exchange.cancel_requested())
        {
            // The patterns name every field. All are plain data, so a boundary does not need
            // dropping, and forgetting it spares the call to `CodecEvent`'s drop glue.
            std::mem::forget(event);
            return CodecCallbackActions::new();
        }
        self.handle_event(event)
    }
}

impl UpstreamDriver {
    fn new(options: OwnedH1Options) -> Result<(Self, OwnedH1UpstreamHandle), HttpError> {
        let OwnedH1Options {
            context,
            transport,
            io,
            http1,
        } = options;
        let io = io.validate()?;
        // As `OwnedH1Session::new`, except that an upstream request body is never drained
        // on drop (the rule of `OwnedH2Session::new` upstream).
        if !context.is_owner_thread()
            || context.is_cancelled()
            || io.maximum_write_size > io.buffer_limit
            || io.maximum_write_size <= CHUNK_OVERHEAD
            || !io.read_timeout.is_zero()
            || !io.stream_read_timeout.is_zero()
            || !io.write_timeout.is_zero()
            || io.drain_request_body_on_drop
            || io.wait_for_http1_response_on_drain
            || io.abort_exchanges_on_eof
            || io.exact_method_tokens
            || transport.byte_event_capability(&context)? != ByteEventCapability::Unsupported
        {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 upstream configuration is not supported",
            ));
        }
        let task = Arc::new(TaskWaker::default());
        let read_wake = Signal::new(&task);
        let write_wake = Signal::new(&task);
        let stream_wake = Signal::new(&task);
        let shared = Rc::new(Shared {
            context: context.clone(),
            wake: Arc::clone(&stream_wake),
            complete: Cell::new(false),
            cancel: Cell::new(false),
            drain: Cell::new(false),
            idle: Cell::new(true),
            used: Cell::new(0),
            spent: Cell::new(false),
            retiring: RefCell::new(None),
            returning_hook: RefCell::new(None),
            committed: Cell::new(0),
            closed: RefCell::new(None),
            observer: RefCell::new(None),
            queued: RefCell::new(None),
            owner: Rc::new(OwnerWake::new(Arc::clone(&task))),
            escalate_drain: Cell::new(false),
            drive: Drive::default(),
        });
        let cancellation = CancellationSource::new();
        let write_cancellation = CancellationSource::new();
        let write_queue = transport.try_write_queue(
            &context,
            io.maximum_write_size,
            io.write_timeout,
            write_cancellation.token(),
        )?;
        let read = ReadSlot::new(
            transport.as_ref(),
            &context,
            io.read_timeout,
            cancellation.token(),
        )?;
        let mut codec =
            Http1xCodec::with_options(TransportDirection::Upstream, false, http1.strict_validation);
        let ordinary_upgrades = http1.upgrade_policy == Http1UpgradePolicy::AsOrdinaryRequest;
        codec.set_upstream_upgrade_as_ordinary(ordinary_upgrades);
        codec.set_unframed_bodiless_requests(io.unframed_bodiless_http1_requests);
        Ok((
            Self {
                context,
                transport,
                shared: Rc::clone(&shared),
                config: io,
                codec: IngressCodec::new(codec),
                exchange: None,
                ingress: BufQueue::new(io.buffer_limit),
                egress: BufQueue::new(io.buffer_limit),
                read,
                write: None,
                write_queue,
                task,
                read_waker: Waker::from(Arc::clone(&read_wake)),
                read_wake,
                write_waker: Waker::from(Arc::clone(&write_wake)),
                write_wake,
                stream_waker: Waker::from(Arc::clone(&stream_wake)),
                stream_wake,
                cancellation,
                write_cancellation,
                eof: false,
                eof_processed: false,
                read_error: None,
                failed_read: None,
                flow_pause: false,
                response_pause: false,
                body_awaits_egress: false,
                fatal: None,
                transport_reset: false,
                write_end: 0,
                summary: UniplexRunSummary::default(),
                budget_yield: None,
                started: false,
                ordinary_upgrades,
                pull_through: Weak::new(),
                teardown: None,
            },
            OwnedH1UpstreamHandle(shared),
        ))
    }

    /// Polls the driver. During teardown this polls native I/O only. Otherwise it runs a turn,
    /// and a panic in that turn is reported and starts the teardown.
    #[inline(always)]
    fn poll_contained(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRunSummary, HttpError>> {
        if self.teardown.is_some() {
            return self.poll_teardown(context);
        }
        self.started = true;
        match catch_unwind(AssertUnwindSafe(|| self.poll(context))) {
            Ok(poll) => poll,
            Err(panic) => {
                report_turn_panic(&self.context, &panic);
                self.poll_panicked(panic);
                Poll::Pending
            }
        }
    }

    /// Starts the teardown after a Driver-scope panic that the caller reported: the connection
    /// aborts, and the run's next polls join native I/O.
    #[cold]
    #[inline(never)]
    fn poll_panicked(&mut self, panic: PanicPayload) {
        discard_panic(panic);
        self.teardown = Some(Box::default());
        if let Err(panic) = guarded(|| {
            self.abort(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H1 upstream callback panicked",
            ))
        }) {
            teardown_panicked(&self.context, panic);
        }
        self.read_wake.mark();
        self.write_wake.mark();
        self.wake_teardown();
    }

    /// Wakes the run for its next teardown poll through the task waker, which every wake of
    /// the driver goes through.
    fn wake_teardown(&self) {
        if let Err(panic) = guarded(|| self.task.wake()) {
            teardown_panicked(&self.context, panic);
        }
    }

    /// One poll of the teardown. Native I/O is polled until it completes, but the codec, the
    /// exchange and admission are not. An operation whose teardown poll panics is polled again
    /// on the next wake, and one whose teardown polls panicked `TEARDOWN_PANIC_LIMIT` times in
    /// total is dropped unfinished. Once every native operation has completed or been dropped,
    /// the run fails with the connection's first error. Every step runs under its own unwind
    /// boundary.
    #[cold]
    #[inline(never)]
    fn poll_teardown(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRunSummary, HttpError>> {
        let task = &self.task;
        if let Err(panic) = guarded(|| task.register(context.waker())) {
            teardown_panicked(&self.context, panic);
        }
        if !self.transport_reset
            && let Err(panic) = guarded(|| self.reset_transport())
        {
            teardown_panicked(&self.context, panic);
        }
        let mut teardown = self.teardown.take().unwrap_or_default();
        self.poll_teardown_read(&mut teardown);
        self.poll_teardown_write(&mut teardown);
        self.teardown = Some(teardown);
        if self.read.is_reading() || self.writes_pending() {
            return Poll::Pending;
        }
        Poll::Ready(Err(self.fatal.take().unwrap_or_else(|| {
            failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H1 upstream callback panicked",
            )
        })))
    }

    fn poll_teardown_read(&mut self, teardown: &mut TeardownState) {
        if !self.read.is_reading() || !self.read_wake.take_ready() {
            return;
        }
        let read = &mut self.read;
        let waker = &self.read_waker;
        let polled = guarded(|| read.poll(&mut Context::from_waker(waker)).map(drop));
        if let Err(panic) = polled
            && self.teardown_poll_panicked(&mut teardown.read_panics, &self.read_wake, panic)
            && let Err(panic) = guarded(|| self.read.abandon())
        {
            teardown_panicked(&self.context, panic);
        }
    }

    fn poll_teardown_write(&mut self, teardown: &mut TeardownState) {
        if !self.write_wake.take_ready() {
            return;
        }
        if let Some(write) = &mut self.write {
            let waker = &self.write_waker;
            let polled = guarded(|| {
                write
                    .as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .map(drop)
            });
            let finished = match polled {
                Ok(polled) => polled.is_ready(),
                Err(panic) => {
                    self.teardown_poll_panicked(&mut teardown.write_panics, &self.write_wake, panic)
                }
            };
            if finished {
                release_field(&self.context, self.write.take());
            }
        }
        if let Some(queue) = &mut self.write_queue {
            let waker = &self.write_waker;
            let polled = guarded(|| {
                let mut context = Context::from_waker(waker);
                while !queue.is_idle() && queue.poll_complete(&mut context).is_ready() {}
            });
            if let Err(panic) = polled
                && self.teardown_poll_panicked(&mut teardown.queue_panics, &self.write_wake, panic)
            {
                release_field(&self.context, self.write_queue.take());
            }
        }
    }

    /// Reports a teardown poll of a native operation that panicked and counts it in `panics`.
    /// Returns whether the operation reached the limit and must be dropped; otherwise `wake` is
    /// marked, so the next teardown poll polls it again.
    #[cold]
    #[inline(never)]
    fn teardown_poll_panicked(&self, panics: &mut u8, wake: &Signal, panic: PanicPayload) -> bool {
        teardown_panicked(&self.context, panic);
        *panics += 1;
        if *panics >= TEARDOWN_PANIC_LIMIT {
            return true;
        }
        wake.mark();
        self.wake_teardown();
        false
    }

    /// Completes a run: the session is finished, and the driver is released.
    #[cold]
    #[inline(never)]
    fn finish_run(
        self,
        result: Result<UniplexRunSummary, HttpError>,
    ) -> Result<UniplexRunSummary, HttpError> {
        self.shared.complete.set(true);
        self.release();
        result
    }

    /// Abandons a run dropped unfinished, or a session dropped unrun. The connection
    /// aborts with `CORO_CANCELLED`, which closes admission, cancels and drops the exchange and
    /// resets the transport, and the driver's fields are dropped, each step under its own unwind
    /// boundary. A started run then panics, because its owner dropped it before its cleanup
    /// joined, unless its poll unwound (`poisoned`), its thread is unwinding, or the worker is in
    /// a forced teardown.
    #[cold]
    #[inline(never)]
    fn abandon(mut self, poisoned: bool) {
        let polling = SharedPolling::enter(&self.shared);
        if let Err(panic) = guarded(|| {
            self.abort(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"owned H1 upstream runner abandoned before cleanup completed",
            ))
        }) {
            teardown_panicked(&self.context, panic);
        }
        let started = self.started;
        self.release();
        drop(polling);
        assert!(
            !started || poisoned || std::thread::panicking() || in_forced_teardown(),
            "started owned H1 upstream runner dropped before native I/O cleanup joined"
        );
    }

    /// Drops every field, in declaration order, each that can run foreign code under its own
    /// unwind boundary, and last the worker context those boundaries report to.
    #[cold]
    #[inline(never)]
    fn release(self) {
        let Self {
            context,
            transport,
            shared,
            config: _,
            codec,
            exchange,
            ingress,
            egress,
            read,
            write,
            write_queue,
            task,
            read_wake,
            read_waker,
            write_wake,
            write_waker,
            stream_wake,
            stream_waker,
            cancellation,
            write_cancellation,
            eof: _,
            eof_processed: _,
            read_error,
            failed_read,
            flow_pause: _,
            response_pause: _,
            body_awaits_egress: _,
            fatal,
            transport_reset: _,
            write_end: _,
            summary: _,
            budget_yield,
            started: _,
            ordinary_upgrades: _,
            pull_through,
            teardown: _,
        } = self;
        release_field(&context, transport);
        release_field(&context, shared);
        release_field(&context, codec);
        release_field(&context, exchange);
        release_field(&context, ingress);
        release_field(&context, egress);
        release_field(&context, read);
        release_field(&context, write);
        release_field(&context, write_queue);
        release_field(&context, task);
        release_field(&context, read_wake);
        release_field(&context, read_waker);
        release_field(&context, write_wake);
        release_field(&context, write_waker);
        release_field(&context, stream_wake);
        release_field(&context, stream_waker);
        release_field(&context, cancellation);
        release_field(&context, write_cancellation);
        release_field(&context, read_error);
        release_field(&context, failed_read);
        release_field(&context, fatal);
        release_field(&context, budget_yield);
        drop(pull_through);
        drop(context);
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRunSummary, HttpError>> {
        self.task.register(context.waker());
        self.task.clear_wake_record();
        if self.budget_yield.is_some() && self.poll_budget_yield(context).is_pending() {
            return Poll::Pending;
        }
        let mut turn = 0;
        while turn < TURN_BUDGET {
            #[cfg(test)]
            count(&DRIVER_TURNS);
            self.observe_cancel();
            let mut progress = false;
            if let Some(wrote) = self.poll_write() {
                progress |= wrote;
                self.observe_cancel();
            }
            if let Some(read) = self.poll_read(&mut turn) {
                progress |= read;
                self.observe_cancel();
            }
            if self.fatal.is_none() && self.poll_admission() {
                progress = true;
                self.observe_cancel();
            }
            if self.fatal.is_none() && !self.ingress.is_empty() && self.exchange.is_none() {
                self.abort(failure(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"owned H1 upstream response without a request",
                ));
            }
            if self.parse_due() {
                match self.parse_input() {
                    Ok(parsed) => progress |= parsed,
                    Err(error) => self.abort(error),
                }
            }
            // While flow control pauses an unfinished response, the bytes read before the
            // failure are not all parsed yet: it waits for them.
            if !(self.flow_pause && self.codec.is_busy())
                && let Some(error) = self.read_error.take()
            {
                self.fail_read(error);
                progress = true;
            }
            if self.fatal.is_none() && self.stream_wake.take_ready() {
                progress = true;
                if let Err(error) = self.poll_exchange() {
                    self.abort(error);
                }
                self.observe_cancel();
            }
            if self.fatal.is_none() && self.settle_exchange() {
                progress = true;
                self.observe_cancel();
            }
            // The write waits while a phase before it can still add to it, since a phase that
            // acted this turn may act again. Otherwise the write starts in place of the next turn.
            // That turn still counts against the budget, which sets when a write is forced, when
            // idleness is noted and when the poll yields.
            let write_waits = progress
                && self.egress.chain_length() < self.config.maximum_write_size
                && turn + 1 < TURN_BUDGET;
            let write_ready =
                !self.transport_reset && !self.egress.is_empty() && self.can_start_write();
            if write_ready && (!write_waits || !self.phases_due_before_write()) {
                if write_waits {
                    turn += 1;
                }
                // A write queue's capacity check, like the capacity callbacks before it, may
                // cancel the connection or the request; the abort discards the egress.
                self.observe_cancel();
                if !self.transport_reset
                    && let Err(error) = self.start_write()
                {
                    self.abort(error);
                }
                progress = true;
            }
            if self.failed_read.is_some() && self.egress.is_empty() && !self.writes_pending() {
                let error = self.failed_read.take().expect("checked above");
                self.abort(error);
            }
            if self.fatal.is_some() && !self.transport_reset {
                self.reset_transport();
                progress = true;
            }
            if self.transport_reset
                && !self.writes_pending()
                && !self.read.is_reading()
                && self.exchange.is_none()
            {
                return Poll::Ready(Err(self
                    .fatal
                    .take()
                    .expect("a reset connection retains its cause")));
            }
            // Bytes after a retired response skip the idle close and fail the connection on the
            // next turn.
            if self.fatal.is_none()
                && self.exchange.is_none()
                && self.ingress.is_empty()
                && self.egress.is_empty()
                && !self.writes_pending()
                && self.shared.closed.borrow().is_some()
                && self.shared.queued.borrow().is_none()
            {
                // The write queue's idle check may cancel the connection.
                self.observe_cancel();
                if self.fatal.is_none() {
                    if let Some(closed) = self.close_idle() {
                        return Poll::Ready(closed);
                    }
                    // Only the outstanding read can progress: its completion wakes the task.
                    if !progress {
                        return Poll::Pending;
                    }
                }
                turn += 1;
                continue;
            }
            if !progress || turn + 1 == TURN_BUDGET {
                self.update_idle();
            }
            if !progress {
                return Poll::Pending;
            }
            turn += 1;
            // A next turn that would not act would note idleness and return, so that happens here
            // instead. After a turn ready to write, the next turn is due: the phase that held the
            // write back still is, or the write marked its signal, or a failure reset the
            // connection.
            if turn < TURN_BUDGET && !write_ready && !self.turn_due() {
                self.update_idle();
                return Poll::Pending;
            }
        }
        self.budget_yield = Some(self.context.host().yield_to_io());
        // A yield that completes on its first poll arranged no wake. The driver wakes itself
        // through its task waker, which records the wake.
        if self.poll_budget_yield(context).is_ready() {
            self.task.wake();
        }
        Poll::Pending
    }

    /// As OwnedH2, idleness is recomputed on every quiescent turn, so a slot returned
    /// uncommitted leaves the connection idle again.
    fn update_idle(&self) {
        self.shared.idle.set(
            self.exchange.is_none()
                && self.shared.used.get() == 0
                && self.egress.is_empty()
                && !self.writes_pending()
                && self.fatal.is_none()
                && !self.transport_reset,
        );
    }

    /// Whether a next turn would act. The terms cover a phase before the write, the write, a
    /// failure or a reset, and the idle close. Each is the condition under which that step acts,
    /// or a wider one, and evaluating them does not run transport or application code.
    fn turn_due(&self) -> bool {
        self.transport_reset
            || self.fatal.is_some()
            || self.phases_due_before_write()
            || if self.egress.is_empty() {
                // The idle check on the write queue is transport code.
                self.exchange.is_none()
                    && self.ingress.is_empty()
                    && self.shared.closed.borrow().is_some()
                    && self.shared.queued.borrow().is_none()
            } else {
                // A queue's capacity check is transport code.
                self.write_queue.is_some() || self.write.is_none()
            }
    }

    /// Whether a phase before the write would act in a next turn. The phases are the write
    /// completion, the read, the exchange, the cancel check, a read start, admission, the check
    /// for a response without a request, the parse, a held read failure and `settle_exchange`.
    /// Each term is the condition under which that phase acts, or a wider one. The terms do not
    /// run transport or application code or change state, so their order affects only cost. The
    /// signals, which most often make a turn due, come first.
    #[inline(always)]
    fn phases_due_before_write(&self) -> bool {
        #[cfg(test)]
        count(&PHASE_CHECKS);
        self.write_wake.is_ready()
            || self.read_wake.is_ready()
            || (self.fatal.is_none() && self.stream_wake.is_ready())
            || (!self.transport_reset
                && match &self.exchange {
                    Some(exchange) => exchange.cancel_requested(),
                    None => self.shared.cancelled(),
                })
            || (!self.read.is_reading() && !self.read.is_drained() && self.read_capacity() != 0)
            || (self.fatal.is_none()
                && (self.admission_due() || (!self.ingress.is_empty() && self.exchange.is_none())))
            || self.parse_due()
            || (self.read_error.is_some() && !(self.flow_pause && self.codec.is_busy()))
            || (self.fatal.is_none() && self.settle_due())
    }

    /// Whether `poll_admission` acts: an observer panic's drain or a peer EOF closes admission,
    /// or a queued submission can commit.
    fn admission_due(&self) -> bool {
        self.shared.escalate_drain.get()
            || (self.eof && self.shared.closed.borrow().is_none())
            || (self.exchange.is_none()
                && self.ingress.is_empty()
                && self.shared.queued.borrow().is_some())
    }

    /// Whether the parse phase acts: the input or the codec changed since a parse consumed all
    /// of its input, and the live exchange's response has something to parse.
    fn parse_due(&self) -> bool {
        self.codec.reparse
            && self.fatal.is_none()
            && self.exchange.is_some()
            && !self.flow_pause
            && !self.response_pause
            && (!self.ingress.is_empty()
                || self.codec.is_busy()
                || (self.eof && !self.eof_processed))
    }

    /// Whether `settle_exchange` acts: it releases the capacity of an exchange whose messages
    /// ended, or retires one whose response is consumed and request written.
    #[inline]
    fn settle_due(&self) -> bool {
        self.exchange.as_ref().is_some_and(|exchange| {
            exchange.request_complete
                && exchange.response_complete
                && ((exchange.status.counted.get() && exchange.status.ingress_succeeded())
                    || (exchange.consumed && exchange.write_boundary <= self.summary.bytes_written))
        })
    }

    /// Run at each phase boundary: callbacks may cancel the session or the live request, and
    /// the phases after them must not act for it.
    #[inline(always)]
    fn observe_cancel(&mut self) {
        if self.transport_reset {
            return;
        }
        let error = match &self.exchange {
            Some(exchange) => exchange.cancel_error(),
            None => self.shared.cancelled().then(cancelled),
        };
        if let Some(error) = error {
            self.abort(error);
        }
    }

    fn poll_budget_yield(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let Some(mut wait) = self.budget_yield.take() else {
            return Poll::Ready(());
        };
        let poll = wait.as_mut().poll(context);
        if poll.is_pending() {
            self.budget_yield = Some(wait);
        }
        poll
    }

    /// Closes admission once the connection cannot take another request, and otherwise commits a
    /// queued submission once the live exchange, if any, has retired. Returns whether either
    /// happened.
    fn poll_admission(&mut self) -> bool {
        if self.shared.escalate_drain.get() {
            self.drain_after_observer_panic();
            return true;
        }
        if self.eof && self.shared.closed.borrow().is_none() {
            self.shared
                .close(failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
            return true;
        }
        // Bytes after a retired response are not a response to the next request. They fail the
        // connection before anything is committed, and a failed read closes admission first.
        if self.exchange.is_some() || !self.ingress.is_empty() || self.read_error.is_some() {
            return false;
        }
        if self.shared.committed.get() != 0
            && self.shared.queued.borrow().is_some()
            && self.read.peer_closed()
        {
            // The read that the peer's close wakes rejects the request as unprocessed.
            return false;
        }
        let Some(mut submission) = self.shared.queued.borrow_mut().take() else {
            return false;
        };
        match catch_unwind(AssertUnwindSafe(|| self.commit(&mut submission))) {
            Ok(Ok(committed)) => self.finish_commit(committed),
            Ok(Err(error)) => self.reject_uncommitted(&mut submission, error),
            Err(panic) => self.commit_panicked(&mut submission, panic),
        }
        true
    }

    /// Closes admission after an observer panicked inside the driver's poll, as
    /// `close_when_idle` closes it, without recording a caller's drain request.
    #[cold]
    #[inline(never)]
    fn drain_after_observer_panic(&self) {
        self.shared.escalate_drain.set(false);
        self.shared.close(failure(
            HttpErrorCode::REFUSED_STREAM,
            b"owned upstream connection is draining",
        ));
    }

    /// Rejects a submission the driver did not commit, under its own unwind boundary.
    #[cold]
    #[inline(never)]
    fn reject_uncommitted(&self, submission: &mut Submission, error: HttpError) {
        if let Err(panic) = guarded(|| submission.reject(error)) {
            report_panic(&self.context, PanicSite::Submission, None, panic);
        }
    }

    /// Reports a panic committing a request, which runs no application code and can leave the
    /// connection's state half updated, rejects the submission, then unwinds to the per-turn
    /// catch, which tears the connection down without reporting the panic again.
    #[cold]
    #[inline(never)]
    fn commit_panicked(&self, submission: &mut Submission, panic: PanicPayload) -> ! {
        report_panic(&self.context, PanicSite::Commit, None, panic);
        self.reject_uncommitted(
            submission,
            failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"upstream request commitment panicked",
            ),
        );
        resume_unwind(Box::new(ReportedPanic));
    }

    /// Runs what a commitment left: the head's byte-event refusals and its encoded-headers
    /// callback, each under its own unwind boundary, then the acknowledgment, which wakes the
    /// submitter. A head with a body to follow whose callback panicked fails the
    /// request, and the connection with it.
    fn finish_commit(&mut self, committed: Committed) {
        let Committed {
            mut headers,
            header_size,
            eom,
            status,
            source,
            id,
        } = committed;
        refuse_request_byte_events(&self.context, id, headers.take_byte_event_registrations());
        if let Err(error) = finish_head_event(&self.context, id, headers, header_size, eom) {
            self.abort(error);
            return;
        }
        status.acknowledge(id, source);
        self.stream_wake.mark();
    }

    fn commit(&mut self, submission: &mut Submission) -> Result<Committed, HttpError> {
        if let Some(error) = submission.status.selected_error() {
            return Err(error);
        }
        // A pull-through exchange's body may hold the drive only of a session run with
        // `run_pull_through`. Under `run` it is rejected before any byte is written.
        let pull_through = if submission.status.pull_through {
            // Defensive: with capacity one, an exchange cannot hold the drive at a commit.
            if self.shared.drive.state.get() != DriveState::Idle {
                return Err(failure(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"owned H1 pull-through submission while another holds the drive",
                ));
            }
            Some(
                self.pull_through
                    .upgrade()
                    .ok_or_else(pull_through_requires_its_run)?,
            )
        } else {
            None
        };
        let headers = submission
            .headers
            .as_mut()
            .expect("queued submission owns headers");
        // As in `UniplexSession`, only HTTP/2 rejects a body on a method that forbids one.
        if !headers.headers().is_request() {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"invalid upstream request head",
            ));
        }
        let method = headers.headers_mut().method().ok().flatten();
        // This driver does not carry tunnels, so a request that can switch protocols is rejected
        // before any byte is written. The codec switches on a CONNECT method, on an `Upgrade`
        // header that a `101` accepts, and on the egress websocket flag, for which it writes the
        // `Upgrade` header itself. Under `AsOrdinaryRequest` the codec sends CONNECT and
        // `Upgrade` requests as ordinary requests, but the flag still requests a handshake.
        if headers.headers().is_egress_websocket_upgrade()
            || (!self.ordinary_upgrades
                && (method == Some(HttpMethod::Connect)
                    || headers.headers().headers().exists(HttpHeaderCode::Upgrade)))
        {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"owned H1 upstream carries no upgrade",
            ));
        }
        let expected_request_length = crate::uniplex::message_content_length(headers.headers());
        // A head that ends the request cannot carry the body it declares.
        if headers.eom() && expected_request_length.is_some_and(|expected| expected != 0) {
            return Err(request_length_mismatch());
        }
        // Under `AsOrdinaryRequest`, a CONNECT or `Upgrade` request whose head does not frame its
        // body the way the codec writes it is rejected before any byte is written.
        if self.ordinary_upgrades
            && !self
                .codec
                .frames_ordinary_request(headers.headers(), None, headers.eom())
        {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"owned H1 upstream request sent as ordinary has ambiguous framing",
            ));
        }
        let head_request = method == Some(HttpMethod::Head);
        let id = self.codec.get_mut().create_stream();
        let signals = Rc::new(IngressSignals {
            source_complete: Cell::new(false),
            completed: Cell::new(None),
            stream: Arc::clone(&self.stream_wake),
            owner: Rc::clone(&self.shared.owner),
            drive: pull_through
                .is_some()
                .then(|| DriveLink(Rc::downgrade(&self.shared))),
            returning: Cell::new(None),
        });
        let source = HttpResponseSource::new_shared(
            id,
            signals.clone(),
            self.config.buffer_limit as u32,
            self.context.host().clone(),
        )
        .map_err(|_| {
            failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid owned H1 response window",
            )
        })?;
        if head_request {
            source.skip_content_length_validation();
        }
        if self.config.deliver_ingress_before_abort {
            source.deliver_ingress_before_abort();
        }
        let producer = source.producer();
        // Parsing resumes once the unread body is below the limit it paused at.
        producer.set_resume_threshold(Some(self.config.ingress_buffer_limit));
        if pull_through.is_some() && !producer.enable_inline_reads() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 pull-through response source refused inline reads",
            ));
        }
        let eom = headers.eom();
        let generated = self.codec.get_mut().generate(
            &mut self.egress,
            EgressFrame::Headers {
                stream: id,
                message: headers.headers(),
                end_stream: eom,
                extra_headers: None,
            },
            &mut NoopCallback,
        )?;
        #[cfg(test)]
        if UPSTREAM_COMMIT_FAULT.take() {
            panic!("owned H1 upstream commit fault");
        }
        let upload = submission
            .source
            .take()
            .expect("queued request source is owned");
        let (transfer, unused_source) = if eom {
            (None, Some(upload))
        } else {
            (
                Some(HttpBodyTransfer::new(
                    upload,
                    id,
                    self.config.maximum_write_size,
                )),
                None,
            )
        };
        let status = Rc::clone(&submission.status);
        status.stream.set(Some(id));
        let committed = self.shared.committed.get();
        status.sequence.set(Some(committed));
        self.shared.committed.set(committed + 1);
        *status.ingress.borrow_mut() = source.ingress_completion();
        submission
            .reservation
            .as_mut()
            .expect("queued request owns a reservation")
            .active = false;
        submission.reservation.take();
        status.counted.set(true);
        status.live.set(true);
        self.shared.spent.set(true);
        self.exchange = Some(Box::new(Exchange {
            status: Rc::clone(&status),
            id,
            signals,
            producer,
            transfer,
            unused_source,
            resume: None,
            cancellation_wake: submission.cancellation_wake.take(),
            response_observer: None,
            request_producer: submission.producer.take(),
            expected_request_length,
            request_body_length: 0,
            request_complete: eom,
            response_complete: false,
            consumed: false,
            write_boundary: self
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64),
        }));
        self.flow_pause = false;
        self.response_pause = false;
        self.codec.get_mut().set_parser_paused(false);
        if let Some(run) = pull_through {
            let signals = Rc::clone(&self.exchange.as_ref().expect("just committed").signals);
            status.link.set(Some(PullThroughLink { run, signals }));
        }
        Ok(Committed {
            headers: submission
                .headers
                .take()
                .expect("queued request headers are owned"),
            header_size: generated.header_size,
            eom,
            status,
            source,
            id,
        })
    }

    fn parse_input(&mut self) -> Result<bool, HttpError> {
        let mut callback = ResponseInput {
            config: &self.config,
            exchange: &mut self.exchange,
            stream_wake: &self.stream_wake,
            flow_pause: &mut self.flow_pause,
            response_pause: &mut self.response_pause,
            failure: None,
        };
        let length = self.ingress.chain_length();
        // A pause can land between a response's last body byte and its completion: the codec
        // completes it on a later, empty, parse.
        let (consumed, mut egress_results) = if length == 0 && !self.codec.is_busy() {
            (0, Vec::new())
        } else {
            #[cfg(test)]
            count(&CODEC_PARSES);
            let result = self.codec.codec.on_ingress_with_callback(
                &self.ingress,
                &mut callback,
                &mut self.egress,
            );
            (result.consumed, result.egress_results)
        };
        // EOF ends a close-delimited response, or fails an unfinished one, once every byte
        // before it is parsed.
        let finishing_eof = self.eof
            && !self.eof_processed
            && !*callback.flow_pause
            && !*callback.response_pause
            && callback.failure.is_none()
            && consumed == length;
        if finishing_eof {
            self.eof_processed = true;
            let result = self
                .codec
                .codec
                .on_ingress_eof(&mut callback, &mut self.egress);
            egress_results.extend(result.egress_results);
            // The codec does not report an EOF before a response begins, so the request fails
            // here, as it does in `UniplexSession`.
            if callback.failure.is_none()
                && callback
                    .exchange
                    .as_ref()
                    .is_some_and(|exchange| !exchange.response_complete)
            {
                callback.fail(failure(
                    HttpErrorCode::TRANSPORT_EOF,
                    b"transport EOF before the response",
                ));
            }
        }
        let failed = callback.failure.take();
        if consumed > length {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"codec consumed beyond the supplied prefix",
            ));
        }
        self.ingress
            .trim_start(consumed)
            .expect("codec consumption was range checked");
        // As downstream: a parse of what remains acts only after a change.
        self.codec.reparse = !self.ingress.is_empty();
        for result in egress_results {
            result?;
        }
        if let Some(error) = failed {
            return Err(error);
        }
        Ok(consumed != 0 || finishing_eof)
    }

    /// Notes the caller's response consumption and serializes the request body.
    fn poll_exchange(&mut self) -> Result<(), HttpError> {
        let Some(exchange) = self.exchange.as_mut() else {
            return Ok(());
        };
        if self.flow_pause
            && exchange
                .producer
                .body_bytes_buffered()
                .is_none_or(|buffered| buffered < self.config.ingress_buffer_limit)
        {
            self.flow_pause = false;
            self.codec.get_mut().set_parser_paused(self.response_pause);
        }
        // A cancel before both messages end closes the connection. After that, the buffered
        // response stays readable, as OwnedH2 leaves a finished stream.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        let mut context = Context::from_waker(&self.stream_waker);
        if let Err(panic) = guarded(|| exchange.poll_response_observer(&mut context))
            && let Some(error) = response_observer_panicked(&self.context, exchange, panic)
        {
            return Err(error);
        }
        // The observer's callbacks may cancel the request.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        if !exchange.consumed && exchange.signals.source_complete.get() {
            exchange.consumed = true;
            let completed = exchange.signals.completed.take();
            if !exchange.response_complete {
                // The caller stopped reading before the response ended: the rest of it cannot
                // be skipped, so the connection closes.
                return Err(match completed {
                    Some(Err(error)) => error,
                    _ => failure(
                        HttpErrorCode::CANCEL,
                        b"response consumer retired before the response ended",
                    ),
                });
            }
        }
        if exchange.request_complete {
            return Ok(());
        }
        let room = self
            .config
            .maximum_write_size
            .saturating_sub(self.egress.chain_length())
            .min(self.egress.remaining_capacity())
            .saturating_sub(CHUNK_OVERHEAD);
        if room == 0 {
            self.body_awaits_egress = true;
            return Ok(());
        }
        let polled = match guarded(|| exchange.poll_upload(&mut context, room as u32)) {
            Ok(polled) => polled,
            Err(panic) => Err(exchange_panicked(
                &self.context,
                PanicSite::UploadBody,
                exchange.id,
                panic,
            )),
        };
        let mut event = match polled {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(()),
            // As OwnedH2, a cancel selected during the failed read is the first cause.
            Err(error) => {
                return Err(exchange
                    .cancel_error()
                    .unwrap_or_else(|| exchange.upload_failed(error)));
            }
        };
        refuse_request_byte_events(
            &self.context,
            exchange.id,
            event.take_byte_event_registrations(),
        );
        // As OwnedH2, the source's read may cancel the request: its event is not serialized.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        let eom = event.eom();
        let request_body_length = exchange
            .request_body_length
            .saturating_add(event.as_body().map_or(0, |body| body.chain_length() as u64));
        // Bytes past the declared length would be read as the next message, and a short body
        // leaves the origin waiting: neither is written.
        if exchange.expected_request_length.is_some_and(|expected| {
            request_body_length > expected || (eom && request_body_length < expected)
        }) {
            return Err(request_length_mismatch());
        }
        exchange.request_body_length = request_body_length;
        match event.into_kind() {
            HttpBodyEventKind::Suspend(resume) => {
                exchange.resume = Some(resume);
            }
            HttpBodyEventKind::Trailers(trailers) => {
                let generated = self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Trailers {
                        stream: exchange.id,
                        trailers: &trailers,
                    },
                    &mut NoopCallback,
                )?;
                // On a `Content-Length` request the codec does not write trailers and leaves its
                // message open, though the request is complete on the wire. Ending the message
                // here does not write anything, and lets the connection carry the next one.
                if generated.bytes_written == 0 {
                    self.codec.get_mut().generate(
                        &mut self.egress,
                        EgressFrame::EndOfMessage {
                            stream: exchange.id,
                        },
                        &mut NoopCallback,
                    )?;
                }
            }
            HttpBodyEventKind::Body(body) => {
                self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Body {
                        stream: exchange.id,
                        body,
                        padding: None,
                        end_stream: eom,
                    },
                    &mut NoopCallback,
                )?;
            }
            HttpBodyEventKind::Padding(length) => {
                self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Padding {
                        stream: exchange.id,
                        length,
                    },
                    &mut NoopCallback,
                )?;
                // HTTP/1.1 does not encode padding, so its EOM ends the message on its own.
                if eom {
                    self.codec.get_mut().generate(
                        &mut self.egress,
                        EgressFrame::EndOfMessage {
                            stream: exchange.id,
                        },
                        &mut NoopCallback,
                    )?;
                }
            }
            _ => {
                return Err(failure(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"owned H1 request event is not supported",
                ));
            }
        }
        self.stream_wake.mark();
        if eom {
            exchange.request_complete = true;
            exchange.transfer = None;
            exchange.write_boundary = self
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64);
            self.note_request_written();
        }
        Ok(())
    }

    /// Notes that a pull-through exchange's request bytes are all written, after which its body
    /// may take the drive.
    #[inline]
    fn note_request_written(&self) {
        if let Some(exchange) = &self.exchange
            && exchange.status.pull_through
            && exchange.request_complete
            && exchange.write_boundary <= self.summary.bytes_written
        {
            self.shared.drive.written.set(Some(exchange.id));
        }
    }

    /// Releases capacity once the request is serialized and the response produced. Retires the
    /// exchange once its response is consumed and its request written. A connection that the
    /// codec leaves reusable between messages then returns its slot, and any other closes
    /// admission. On such a connection, while only the consumer's completion is left, the
    /// exchange is published as retiring (`Shared::retiring`).
    fn settle_exchange(&mut self) -> bool {
        let Some(exchange) = self.exchange.as_mut() else {
            return false;
        };
        let produced = exchange.request_complete
            && exchange.response_complete
            && exchange.status.ingress_succeeded();
        if produced {
            // The response is fully produced: a held drive returns to the run before the
            // release's wake, which then reaches only the run.
            self.shared.hand_back(exchange.id);
            exchange.status.release();
        }
        let written = exchange.write_boundary <= self.summary.bytes_written;
        if !(exchange.request_complete
            && exchange.response_complete
            && exchange.consumed
            && written)
        {
            // Only the consumer's completion is left. From then on, the slot admits the next
            // request, which the turn that the completion wakes commits after retiring this one.
            if produced
                && written
                && self.codec.is_reusable()
                && !self.codec.is_busy()
                && self.shared.retiring.borrow().is_none()
            {
                *self.shared.retiring.borrow_mut() = Some(Rc::clone(&exchange.signals));
                if let Some(hook) = self.shared.returning_hook.borrow().clone() {
                    exchange.signals.returning.set(Some(hook));
                }
            }
            return false;
        }
        self.shared.retiring.take();
        let exchange = self.exchange.take().expect("the exchange was present");
        let id = exchange.id;
        if let Err(panic) = guarded(|| drop(exchange)) {
            report_panic(
                &self.context,
                PanicSite::ExchangeRetirement,
                Some(id),
                panic,
            );
        }
        self.shared.idle.set(true);
        // A codec that still holds a message open cannot start the next one.
        if self.codec.is_reusable() && !self.codec.is_busy() {
            self.shared.return_slot();
        } else {
            self.shared.close(failure(
                HttpErrorCode::REFUSED_STREAM,
                b"owned H1 upstream connection is not reusable",
            ));
        }
        true
    }

    fn poll_read(&mut self, turn: &mut usize) -> Option<bool> {
        let mut read = self.poll_read_once()?;
        let progress = read != ReadPoll::Pending;
        while read == ReadPoll::Data && *turn + 1 < TURN_BUDGET {
            read = match self.poll_read_once() {
                Some(ReadPoll::Pending) | None => break,
                Some(next) => next,
            };
            *turn += 1;
        }
        Some(progress)
    }

    /// How much a new read may take: 0 while a read cannot start, because the connection read
    /// EOF or a failure or is ending, or while the ingress buffer is full.
    fn read_capacity(&self) -> usize {
        if self.eof
            || self.read_error.is_some()
            || self.failed_read.is_some()
            || self.fatal.is_some()
            || self.cancellation.is_cancellation_requested()
        {
            return 0;
        }
        self.config
            .maximum_read_size
            .min(self.ingress.remaining_capacity())
    }

    fn poll_read_once(&mut self) -> Option<ReadPoll> {
        // Until a drained reader's waker fires, a call checks only that waker, as for an
        // outstanding read. This also applies inside a pull-through body's drive, where that
        // waker wakes the holder.
        if self.read.is_drained() && !self.read_wake.is_ready() {
            return None;
        }
        let polled = if self.read.is_reading() {
            if !self.read_wake.take_ready() {
                return None;
            }
            self.read.poll(&mut Context::from_waker(&self.read_waker))
        } else {
            #[cfg(test)]
            count(&READ_START_CHECKS);
            let maximum = self.read_capacity();
            // A drained reader's next read waits for its waker (`ReadSlot::is_drained`).
            if maximum == 0 || (!self.read_wake.take_ready() && self.read.is_drained()) {
                return None;
            }
            self.read.poll_start(
                self.transport.as_ref(),
                &self.context,
                &mut Context::from_waker(&self.read_waker),
                maximum,
                self.config.read_allocation_size.min(maximum),
            )
        };
        let Poll::Ready(result) = polled else {
            return Some(ReadPoll::Pending);
        };
        match result {
            Ok(read) if read.bytes_read() == 0 && !read.is_eof() => return Some(ReadPoll::Empty),
            Ok(read) => {
                self.summary.bytes_read = self
                    .summary
                    .bytes_read
                    .saturating_add(read.bytes_read() as u64);
                if read.is_eof() {
                    self.eof = true;
                }
                self.codec.reparse = true;
                let mut data = read.into_data();
                if self.ingress.append(&mut data).is_err() {
                    self.abort(failure(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"transport read exceeded requested ingress capacity",
                    ));
                }
            }
            Err(error) => {
                if self.fatal.is_none() && !self.cancellation.is_cancellation_requested() {
                    self.read_error = Some(error);
                }
            }
        }
        Some(ReadPoll::Data)
    }

    fn writes_pending(&self) -> bool {
        self.write.is_some()
            || self
                .write_queue
                .as_ref()
                .is_some_and(|queue| !queue.is_idle())
    }

    fn can_start_write(&self) -> bool {
        match &self.write_queue {
            Some(queue) => queue.can_write(
                self.egress
                    .chain_length()
                    .min(self.config.maximum_write_size),
            ),
            None => self.write.is_none(),
        }
    }

    fn start_write(&mut self) -> Result<(), HttpError> {
        let data = self.egress.split_at_most(self.config.maximum_write_size);
        self.summary.bytes_scheduled = self
            .summary
            .bytes_scheduled
            .saturating_add(data.chain_length() as u64);
        self.write_end = self.summary.bytes_scheduled;
        if std::mem::take(&mut self.body_awaits_egress) {
            self.stream_wake.mark();
        }
        if let Some(queue) = &mut self.write_queue {
            queue.start_write(data)?;
        } else {
            self.write = Some(self.transport.write(
                &self.context,
                data,
                ByteEventWriteFlags::default(),
                self.config.write_timeout,
                self.write_cancellation.token(),
            ));
        }
        self.write_wake.mark();
        Ok(())
    }

    fn poll_write(&mut self) -> Option<bool> {
        if !self.write_wake.take_ready() {
            return None;
        }
        let mut context = Context::from_waker(&self.write_waker);
        let mut progress = false;
        let mut failed = None;
        if let Some(queue) = &mut self.write_queue {
            while !queue.is_idle() {
                let Poll::Ready(completion) = queue.poll_complete(&mut context) else {
                    break;
                };
                progress = true;
                let bytes = match &completion.result {
                    Ok(info) => info.bytes_written,
                    Err(error) => error.bytes_written(),
                };
                self.summary.bytes_written =
                    self.summary.bytes_written.saturating_add(bytes as u64);
                match completion.result {
                    Ok(_) if bytes == completion.requested_bytes => {}
                    Ok(_) => {
                        failed.get_or_insert(failure(
                            HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            b"transport reported a short successful write",
                        ));
                    }
                    Err(error) => {
                        failed.get_or_insert_with(|| error.error().clone());
                    }
                }
            }
        } else if let Some(write) = &mut self.write {
            let Poll::Ready(result) = write.as_mut().poll(&mut context) else {
                return Some(false);
            };
            self.write.take();
            progress = true;
            let bytes = match &result {
                Ok(info) => info.bytes_written,
                Err(error) => error.bytes_written(),
            };
            self.summary.bytes_written = self.summary.bytes_written.saturating_add(bytes as u64);
            match result {
                Err(error) => failed = Some(error.error().clone()),
                Ok(_) if self.summary.bytes_written != self.write_end => {
                    failed = Some(failure(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"transport reported a short successful write",
                    ));
                }
                Ok(_) => {}
            }
        }
        if let Some(error) = failed {
            self.abort(error);
        }
        if progress {
            self.note_request_written();
        }
        Some(progress)
    }

    /// `None` while the outstanding read has yet to observe cancellation.
    fn close_idle(&mut self) -> Option<Result<UniplexRunSummary, HttpError>> {
        if self.cancellation.request_cancellation() {
            self.read_wake.mark();
        }
        if self.read.is_reading() {
            return None;
        }
        Some(
            self.transport
                .shutdown_write(&self.context)
                .map(|()| self.summary),
        )
    }

    #[cold]
    fn reset_transport(&mut self) {
        self.read_wake.mark();
        self.write_wake.mark();
        if !self.transport_reset {
            self.transport_reset = true;
            self.summary.reset = true;
            let site = PanicSite::Transport;
            if let Err(panic) = guarded(|| self.cancellation.request_cancellation()) {
                report_panic(&self.context, site, None, panic);
            }
            if let Err(panic) = guarded(|| self.write_cancellation.request_cancellation()) {
                report_panic(&self.context, site, None, panic);
            }
            match guarded(|| self.transport.close_with_reset(&self.context)) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.fatal.get_or_insert(error);
                }
                Err(panic) => report_panic(&self.context, site, None, panic),
            }
        }
    }

    /// Fails the connection for a failed read. As in `UniplexSession`, admission closes, the
    /// live exchange is cancelled immediately, reading stops, and the transport resets once the
    /// request bytes already serialized are written, even if a callback panics along the way.
    #[cold]
    fn fail_read(&mut self, error: HttpError) {
        if self.fatal.is_some() || (self.egress.is_empty() && !self.writes_pending()) {
            self.abort(error);
            return;
        }
        self.failed_read = Some(error.clone());
        self.close_and_cancel(&error);
    }

    /// Fails the connection: the live exchange is cancelled and dropped, admission closes,
    /// pending egress is discarded, and the transport is reset once outstanding operations are
    /// cancelled. A failed read that was waiting for its writes stays the first cause.
    #[cold]
    fn abort(&mut self, error: HttpError) {
        let first = self.failed_read.take();
        self.fatal
            .get_or_insert_with(|| first.unwrap_or_else(|| error.clone()));
        self.close_and_cancel(&error);
        self.egress = BufQueue::new(self.config.buffer_limit);
        self.reset_transport();
    }

    /// Closes admission with `error`, then cancels and drops the live exchange with it. Each
    /// step runs under its own unwind boundary.
    #[cold]
    fn close_and_cancel(&mut self, error: &HttpError) {
        // A held drive returns to the run future first: the polls the run still needs must not
        // wait for the holder's reads, and the wakes below reach only the run.
        if let Some(exchange) = &self.exchange {
            let stream = exchange.id;
            if let Err(panic) = guarded(|| self.shared.revoke(Some(stream))) {
                report_panic(&self.context, PanicSite::Transport, None, panic);
            }
        }
        // Admission closes first, so the exchange's capacity release does not announce capacity.
        if let Err(panic) = guarded(|| self.shared.close(error.clone())) {
            report_panic(&self.context, PanicSite::UpstreamObserver, None, panic);
        }
        if let Some(signals) = self.shared.retiring.take() {
            signals.returning.take();
        }
        if let Some(mut exchange) = self.exchange.take() {
            let id = Some(exchange.id);
            let site = PanicSite::ExchangeRetirement;
            if let Err(panic) = guarded(|| exchange.cancel(error)) {
                report_panic(&self.context, site, id, panic);
            }
            if let Err(panic) = guarded(|| drop(exchange)) {
                report_panic(&self.context, site, id, panic);
            }
        }
    }
}

/// Reports a panic in a request's response observer and returns the error that fails the
/// request, unless both of its messages ended: a finished exchange keeps its readable response,
/// as it does a request cancel.
#[cold]
#[inline(never)]
fn response_observer_panicked(
    context: &WorkerContext,
    exchange: &Exchange,
    panic: PanicPayload,
) -> Option<HttpError> {
    let error = exchange_panicked(context, PanicSite::ResponseObserver, exchange.id, panic);
    (!(exchange.request_complete && exchange.response_complete)).then_some(error)
}
