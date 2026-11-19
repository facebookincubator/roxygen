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

/// Failure to certify cleanup, independent of the HTTP exchange's outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExchangeCompletionError {
    #[error("native exchange cleanup could not be confirmed")]
    CleanupUnconfirmed,
}

pub type LocalExchangeCompletionFuture =
    Pin<Box<dyn Future<Output = Result<(), ExchangeCompletionError>> + 'static>>;

/// Observation supplied by the worker-local exchange owner, not cleanup ownership.
pub trait ExchangeCompletionObserver: 'static {
    fn wait(&self) -> LocalExchangeCompletionFuture;
}

/// A sticky native-cleanup observation. Dropping it never cancels cleanup.
///
/// Success does not imply HTTP success, transport flush or closure, application
/// task joins, or physical worker joins. A handler must not await its own receipt.
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
