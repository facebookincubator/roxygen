/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use thiserror::Error;

/// Error returned when exchange cleanup cannot be confirmed. It does not depend
/// on the HTTP exchange's outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExchangeCompletionError {
    #[error("native exchange cleanup could not be confirmed")]
    CleanupUnconfirmed,
}

pub type LocalExchangeCompletionFuture =
    Pin<Box<dyn Future<Output = Result<(), ExchangeCompletionError>> + 'static>>;

/// Implemented by the worker-local exchange owner to report when cleanup
/// finishes. It does not own the cleanup.
pub trait ExchangeCompletionObserver: 'static {
    fn wait(&self) -> LocalExchangeCompletionFuture;
}

/// Reports when an exchange's native cleanup has finished. Once complete, it
/// stays complete. Dropping it does not cancel the cleanup.
///
/// Success does not mean that the HTTP exchange succeeded or that the transport
/// was flushed or closed. A handler must not await its own exchange's completion.
#[derive(Clone)]
pub struct ExchangeCompletion(Rc<dyn ExchangeCompletionObserver>);

impl ExchangeCompletion {
    pub fn new(observer: Rc<dyn ExchangeCompletionObserver>) -> Self {
        Self(observer)
    }

    pub fn wait(&self) -> LocalExchangeCompletionFuture {
        self.0.wait()
    }

    pub(crate) fn same_exchange(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}
