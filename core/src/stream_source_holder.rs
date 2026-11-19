/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use roxygen_runtime::cancellation::CancellationToken;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::ids::StreamId;
use crate::source::LocalSourceFuture;
use crate::stream_source::HttpStreamSource;
use crate::stream_source::HttpStreamSourceCallback;
use crate::stream_source::HttpStreamSourceProducer;
use crate::stream_source::StreamSourceCreateError;

struct CapacityWaiter {
    id: u64,
    waker: Waker,
    signalled: bool,
}

struct HolderState {
    source: Option<HttpStreamSource>,
    producer: HttpStreamSourceProducer,
    self_reference: Option<Rc<HttpStreamSourceHolder>>,
    completion_error: Option<HttpError>,
    capacity_waiters: Vec<CapacityWaiter>,
    next_waiter_id: u64,
}

/// Owns an asynchronous stream source and its egress-capacity waiters.
pub struct HttpStreamSourceHolder {
    egress_buffer_size: u32,
    state: RefCell<HolderState>,
}

impl HttpStreamSourceHolder {
    pub fn make(
        id: Option<StreamId>,
        egress_buffer_size: u32,
    ) -> Result<Rc<Self>, StreamSourceCreateError> {
        let source = HttpStreamSource::new_async(id, None, egress_buffer_size)?;
        let producer = source.producer();
        let holder = Rc::new(Self {
            egress_buffer_size,
            state: RefCell::new(HolderState {
                source: Some(source),
                producer,
                self_reference: None,
                completion_error: None,
                capacity_waiters: Vec::new(),
                next_waiter_id: 1,
            }),
        });
        let callback = HolderCallback {
            holder: Rc::downgrade(&holder),
        };
        holder
            .state
            .borrow()
            .source
            .as_ref()
            .expect("a newly created holder retains its source")
            .set_callback(Some(Box::new(callback)));
        Ok(holder)
    }

    pub fn with_default_capacity(
        id: Option<StreamId>,
    ) -> Result<Rc<Self>, StreamSourceCreateError> {
        Self::make(id, 65_535)
    }

    /// Retains the holder until its source completion callback runs.
    pub fn start(self: &Rc<Self>) {
        let mut state = self.state.borrow_mut();
        if state.self_reference.is_none() {
            state.self_reference = Some(Rc::clone(self));
        }
    }

    pub fn is_live(&self) -> bool {
        self.state.borrow().source.is_some()
    }

    pub fn producer(&self) -> HttpStreamSourceProducer {
        self.state.borrow().producer.clone()
    }

    pub fn completion_error(&self) -> Option<HttpError> {
        self.state.borrow().completion_error.clone()
    }

    pub fn read_header_event(&self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.read_header_event_with_cancellation(CancellationToken::never())
    }

    pub fn read_header_event_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.state.borrow().source.as_ref().map_or_else(
            || Box::pin(std::future::ready(Err(source_unavailable_error()))) as _,
            |source| source.read_header_event_with_cancellation(cancellation),
        )
    }

    pub fn read_body_event(
        &self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.read_body_event_with_cancellation(maximum, CancellationToken::never())
    }

    pub fn read_body_event_with_cancellation(
        &self,
        maximum: u32,
        cancellation: CancellationToken,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.state.borrow().source.as_ref().map_or_else(
            || Box::pin(std::future::ready(Err(source_unavailable_error()))) as _,
            |source| source.read_body_event_with_cancellation(maximum, cancellation),
        )
    }

    pub fn stop_reading(&self, error: Option<HttpErrorCode>) {
        let producer = self.producer();
        producer.stop_reading(error);
    }

    pub fn await_egress_buffer(self: &Rc<Self>) -> CapacityWait {
        CapacityWait {
            holder: Rc::clone(self),
            waiter_id: None,
        }
    }

    fn source_complete(&self, error: Option<HttpError>) {
        let (source, self_reference, wakers) = {
            let mut state = self.state.borrow_mut();
            state.completion_error = error;
            let wakers = state
                .capacity_waiters
                .iter_mut()
                .map(|waiter| {
                    waiter.signalled = true;
                    waiter.waker.clone()
                })
                .collect::<Vec<_>>();
            (state.source.take(), state.self_reference.take(), wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        drop(source);
        drop(self_reference);
    }

    fn window_open(&self) {
        let wakers = self
            .state
            .borrow_mut()
            .capacity_waiters
            .iter_mut()
            .map(|waiter| {
                waiter.signalled = true;
                waiter.waker.clone()
            })
            .collect::<Vec<_>>();
        for waker in wakers {
            waker.wake();
        }
    }

    fn egress_blocked(&self) -> bool {
        self.state
            .borrow()
            .source
            .as_ref()
            .is_some_and(|source| source.body_bytes_buffered() >= self.egress_buffer_size)
    }
}

struct HolderCallback {
    holder: Weak<HttpStreamSourceHolder>,
}

impl HttpStreamSourceCallback for HolderCallback {
    fn window_open(&mut self, _id: StreamId) {
        if let Some(holder) = self.holder.upgrade() {
            holder.window_open();
        }
    }

    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        if let Some(holder) = self.holder.upgrade() {
            holder.source_complete(error);
        }
    }
}

pub struct CapacityWait {
    holder: Rc<HttpStreamSourceHolder>,
    waiter_id: Option<u64>,
}

impl Future for CapacityWait {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(id) = self.waiter_id {
            let signalled = self
                .holder
                .state
                .borrow()
                .capacity_waiters
                .iter()
                .find(|waiter| waiter.id == id)
                .is_some_and(|waiter| waiter.signalled);
            if signalled {
                self.unregister();
                return Poll::Ready(());
            }
        }
        if !self.holder.egress_blocked() {
            self.unregister();
            return Poll::Ready(());
        }
        let mut state = self.holder.state.borrow_mut();
        if let Some(id) = self.waiter_id {
            let waiter = state
                .capacity_waiters
                .iter_mut()
                .find(|waiter| waiter.id == id)
                .expect("a registered capacity waiter remains installed");
            if !waiter.waker.will_wake(context.waker()) {
                waiter.waker = context.waker().clone();
            }
        } else {
            let id = state.next_waiter_id;
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
            state.capacity_waiters.push(CapacityWaiter {
                id,
                waker: context.waker().clone(),
                signalled: false,
            });
            drop(state);
            self.waiter_id = Some(id);
        }
        Poll::Pending
    }
}

impl CapacityWait {
    fn unregister(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        self.holder
            .state
            .borrow_mut()
            .capacity_waiters
            .retain(|waiter| waiter.id != id);
    }
}

impl Drop for CapacityWait {
    fn drop(&mut self) {
        self.unregister();
    }
}

fn source_unavailable_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::DROPPED,
        b"HTTP stream source holder no longer has a source",
    )
}
