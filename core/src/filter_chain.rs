/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::source::HttpSource;
use crate::source_filter::FilterMutationError;
use crate::source_filter::HttpSourceFilter;
use crate::source_filter::HttpSourceFilterHandle;
use crate::source_filter::RejectedFilterSource;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::SourceReleaseError;

/// An owning chain of source filters with independent front and end insertion.
#[derive(Default)]
pub struct FilterChain {
    head: HttpSourceHolder,
    tail: Option<HttpSourceFilterHandle>,
}

impl FilterChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_source(&mut self, source: Box<dyn HttpSource>) -> Result<(), RejectedFilterSource> {
        self.discard_expired_tail();
        if let Some(tail) = &self.tail {
            let head = match self.head.release() {
                Ok(Some(head)) => head,
                Ok(None) => unreachable!("a live tail requires an owned filter chain"),
                Err(SourceReleaseError::ReadInProgress) => {
                    return Err(RejectedFilterSource::new(
                        FilterMutationError::ReadInProgress,
                        source,
                    ));
                }
            };
            let result = tail.set_source(source);
            self.head.set_source(head);
            result?;
        } else {
            let mut previous = match self.head.release() {
                Ok(previous) => previous,
                Err(SourceReleaseError::ReadInProgress) => {
                    return Err(RejectedFilterSource::new(
                        FilterMutationError::ReadInProgress,
                        source,
                    ));
                }
            };
            if let Some(previous) = previous.as_deref_mut() {
                previous.stop_reading(None);
            }
            self.head.set_source(source);
        }
        Ok(())
    }

    pub fn readable(&self) -> bool {
        match &self.tail {
            Some(tail) => tail.readable(),
            None => self.head.readable(),
        }
    }

    pub fn head(&mut self) -> Option<&mut HttpSourceHolder> {
        self.readable().then_some(&mut self.head)
    }

    pub fn insert_front(
        &mut self,
        filter: impl Into<HttpSourceFilter>,
    ) -> Result<(), FilterMutationError> {
        self.discard_expired_tail();
        let mut filter = filter.into();
        let filter_tail = filter.tail_handle();
        if self.tail.is_some() || self.head.readable() {
            let source = self
                .head
                .release()
                .map_err(|SourceReleaseError::ReadInProgress| FilterMutationError::ReadInProgress)?
                .expect("an existing filter or readable head must be owned by the head holder");
            match filter.set_source(source) {
                Ok(()) => {}
                Err(rejection) => return Err(rejection.reason()),
            }
        }
        self.head.set_source(Box::new(filter));
        if self.tail.is_none() {
            self.tail = Some(filter_tail);
        }
        Ok(())
    }

    pub fn insert_end(
        &mut self,
        filter: impl Into<HttpSourceFilter>,
    ) -> Result<(), FilterMutationError> {
        self.discard_expired_tail();
        let mut filter = filter.into();
        let filter_tail = filter.tail_handle();
        let Some(tail) = &self.tail else {
            return self.insert_front(filter);
        };

        if let Some(source) = tail
            .release()
            .expect("the tail remains live while the head owns the filter chain")?
        {
            match filter.set_source(source) {
                Ok(()) => {}
                Err(rejection) => return Err(rejection.reason()),
            }
        }
        match tail.set_source(Box::new(filter)) {
            Ok(()) => {}
            Err(rejection) => return Err(rejection.reason()),
        }
        self.tail = Some(filter_tail);
        Ok(())
    }

    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, SourceReleaseError> {
        let source = self.head.release()?;
        self.tail = None;
        Ok(source)
    }

    fn discard_expired_tail(&mut self) {
        if self.tail.as_ref().is_some_and(|tail| !tail.is_live()) {
            self.tail = None;
        }
    }
}
