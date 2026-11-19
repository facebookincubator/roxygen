/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::poll_fn;
use std::task::Poll;

use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use thiserror::Error;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEventKind;
use crate::events::HttpPushEvent;
use crate::events::ResumeStatus;
use crate::filter_chain::FilterChain;
use crate::headers::HttpHeaders;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_filter::FilterMutationError;
use crate::source_filter::HttpSourceFilter;
use crate::source_filter::RejectedFilterSource;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::SourceReleaseError;

pub type SourceReaderControlFuture = LocalSourceFuture<Result<bool, HttpError>>;
pub type SourceReaderUnitFuture = LocalSourceFuture<Result<(), HttpError>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorContext {
    Headers,
    Body,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CallbackRegistrationError {
    #[error("The async callback has been already set")]
    AsyncAlreadySet,
    #[error("The plain callback has been already set")]
    PlainAlreadySet,
}

type PreReadFn = Box<dyn FnMut() -> bool>;
type AsyncPreReadFn = Box<dyn FnMut() -> SourceReaderControlFuture>;
type HeaderFn = Box<dyn FnMut(HttpMessage, bool, bool) -> bool>;
type AsyncHeaderFn = Box<dyn FnMut(HttpMessage, bool, bool) -> SourceReaderControlFuture>;
type BodyFn = Box<dyn FnMut(BufQueue, bool) -> bool>;
type AsyncBodyFn = Box<dyn FnMut(BufQueue, bool) -> SourceReaderControlFuture>;
type PushPromiseFn = Box<dyn FnMut(HttpPushEvent, bool) -> bool>;
type AsyncPushPromiseFn = Box<dyn FnMut(HttpPushEvent, bool) -> SourceReaderControlFuture>;
type TrailerFn = Box<dyn FnMut(HttpHeaders)>;
type AsyncTrailerFn = Box<dyn FnMut(HttpHeaders) -> SourceReaderUnitFuture>;
type ErrorFn = Box<dyn FnMut(ErrorContext, HttpError)>;
type FallibleErrorFn = Box<dyn FnMut(ErrorContext, HttpError) -> Result<(), HttpError>>;
type AsyncErrorFn = Box<dyn FnMut(ErrorContext, HttpError) -> SourceReaderUnitFuture>;

#[derive(Default)]
pub struct HttpSourceReader {
    filter_chain: FilterChain,
    pre_read_fn: Option<PreReadFn>,
    header_fn: Option<HeaderFn>,
    body_fn: Option<BodyFn>,
    promise_fn: Option<PushPromiseFn>,
    trailer_fn: Option<TrailerFn>,
    error_fn: Option<ErrorFn>,
    fallible_error_fn: Option<FallibleErrorFn>,
    async_pre_read_fn: Option<AsyncPreReadFn>,
    async_header_fn: Option<AsyncHeaderFn>,
    async_body_fn: Option<AsyncBodyFn>,
    async_promise_fn: Option<AsyncPushPromiseFn>,
    async_trailer_fn: Option<AsyncTrailerFn>,
    async_error_fn: Option<AsyncErrorFn>,
}

impl HttpSourceReader {
    pub const CONTINUE: bool = false;
    pub const CANCEL: bool = true;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_source(source: Box<dyn HttpSource>) -> Self {
        let mut reader = Self::new();
        reader
            .filter_chain
            .set_source(source)
            .unwrap_or_else(|_| unreachable!("an empty reader accepts its first source"));
        reader
    }

    pub fn set_source(
        &mut self,
        source: Box<dyn HttpSource>,
    ) -> Result<&mut Self, RejectedFilterSource> {
        self.filter_chain.set_source(source)?;
        Ok(self)
    }

    pub fn insert_filter(
        &mut self,
        filter: impl Into<HttpSourceFilter>,
    ) -> Result<&mut Self, FilterMutationError> {
        self.filter_chain.insert_end(filter)?;
        Ok(self)
    }

    pub fn pre_read(
        &mut self,
        callback: impl FnMut() -> bool + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_pre_read_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.pre_read_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn pre_read_async(
        &mut self,
        callback: impl FnMut() -> SourceReaderControlFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.pre_read_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_pre_read_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_headers(
        &mut self,
        callback: impl FnMut(HttpMessage, bool, bool) -> bool + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_header_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.header_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_headers_async(
        &mut self,
        callback: impl FnMut(HttpMessage, bool, bool) -> SourceReaderControlFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.header_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_header_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_body(
        &mut self,
        callback: impl FnMut(BufQueue, bool) -> bool + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_body_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.body_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_body_async(
        &mut self,
        callback: impl FnMut(BufQueue, bool) -> SourceReaderControlFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.body_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_body_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_push_promise(
        &mut self,
        callback: impl FnMut(HttpPushEvent, bool) -> bool + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_promise_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.promise_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_push_promise_async(
        &mut self,
        callback: impl FnMut(HttpPushEvent, bool) -> SourceReaderControlFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.promise_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_promise_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_trailers(
        &mut self,
        callback: impl FnMut(HttpHeaders) + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_trailer_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.trailer_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_trailers_async(
        &mut self,
        callback: impl FnMut(HttpHeaders) -> SourceReaderUnitFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.trailer_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_trailer_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_error(
        &mut self,
        callback: impl FnMut(ErrorContext, HttpError) + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.async_error_fn.is_some() || self.fallible_error_fn.is_some() {
            return Err(CallbackRegistrationError::AsyncAlreadySet);
        }
        self.error_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_error_result(
        &mut self,
        callback: impl FnMut(ErrorContext, HttpError) -> Result<(), HttpError> + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.error_fn.is_some() || self.async_error_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.fallible_error_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub fn on_error_async(
        &mut self,
        callback: impl FnMut(ErrorContext, HttpError) -> SourceReaderUnitFuture + 'static,
    ) -> Result<&mut Self, CallbackRegistrationError> {
        if self.error_fn.is_some() || self.fallible_error_fn.is_some() {
            return Err(CallbackRegistrationError::PlainAlreadySet);
        }
        self.async_error_fn = Some(Box::new(callback));
        Ok(self)
    }

    pub async fn read(&mut self, maximum_body_size: u32) -> Result<(), HttpError> {
        self.read_with_cancellation(maximum_body_size, CancellationToken::never())
            .await
    }

    pub async fn read_with_cancellation(
        &mut self,
        maximum_body_size: u32,
        cancellation: CancellationToken,
    ) -> Result<(), HttpError> {
        let source = self
            .filter_chain
            .release()
            .map_err(source_release_error)?
            .ok_or_else(missing_source_error)?;
        let mut head = HttpSourceHolder::new(source);

        loop {
            if cancellation.is_cancellation_requested() {
                return Ok(());
            }
            match self.run_pre_read(cancellation.clone()).await {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(stop)) if stop => return Ok(()),
                AwaitOutcome::Ready(Ok(_)) => {}
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Headers, error, cancellation.clone())
                        .await;
                }
            }
            if cancellation.is_cancellation_requested() {
                return Ok(());
            }

            let header = match await_or_cancel(head.read_header_event(), cancellation.clone()).await
            {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(header)) => header,
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Headers, error, cancellation.clone())
                        .await;
                }
            };
            let mut header = header;
            let final_headers = match header.is_final() {
                Ok(final_headers) => final_headers,
                Err(error) => {
                    return self
                        .handle_error(
                            ErrorContext::Headers,
                            HttpError::with_message(
                                HttpErrorCode::MESSAGE_ERROR,
                                error.to_string().as_bytes(),
                            ),
                            cancellation.clone(),
                        )
                        .await;
                }
            };
            let eom = header.eom();
            let callback = self
                .run_header(
                    header.into_headers(),
                    final_headers,
                    eom,
                    cancellation.clone(),
                )
                .await;
            let stop = match callback {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(stop)) => stop,
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Headers, error, cancellation.clone())
                        .await;
                }
            };
            if eom {
                return Ok(());
            }
            if stop || cancellation.is_cancellation_requested() {
                return Ok(());
            }
            if final_headers {
                break;
            }
        }

        loop {
            if cancellation.is_cancellation_requested() {
                return Ok(());
            }
            match self.run_pre_read(cancellation.clone()).await {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(stop)) if stop => return Ok(()),
                AwaitOutcome::Ready(Ok(_)) => {}
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Body, error, cancellation.clone())
                        .await;
                }
            }
            if cancellation.is_cancellation_requested() {
                return Ok(());
            }

            let event = match await_or_cancel(
                head.read_body_event(maximum_body_size),
                cancellation.clone(),
            )
            .await
            {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(event)) => event,
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Body, error, cancellation.clone())
                        .await;
                }
            };
            let eom = event.eom();
            let callback = match event.into_kind() {
                HttpBodyEventKind::Body(body) => {
                    if body.is_empty() && !eom {
                        AwaitOutcome::Ready(Err(HttpError::with_message(
                            HttpErrorCode::INVALID_STATE_TRANSITION,
                            b"non-terminal body event must contain body bytes",
                        )))
                    } else {
                        self.run_body(body, eom, cancellation.clone()).await
                    }
                }
                HttpBodyEventKind::PushPromise(push) => {
                    self.run_push_promise(push, eom, cancellation.clone()).await
                }
                HttpBodyEventKind::Trailers(trailers) => {
                    self.run_trailers(trailers, cancellation.clone()).await
                }
                HttpBodyEventKind::Suspend(resume) => {
                    let status = match await_or_cancel(resume, cancellation.clone()).await {
                        AwaitOutcome::Cancelled => return Ok(()),
                        AwaitOutcome::Ready(status) => status,
                    };
                    if status == ResumeStatus::Cancelled {
                        return Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"Read cancelled",
                        ));
                    }
                    AwaitOutcome::Ready(Ok(false))
                }
                HttpBodyEventKind::Upgrade | HttpBodyEventKind::Padding(_) => {
                    AwaitOutcome::Ready(Ok(false))
                }
            };
            let stop = match callback {
                AwaitOutcome::Cancelled => return Ok(()),
                AwaitOutcome::Ready(Ok(stop)) => stop,
                AwaitOutcome::Ready(Err(error)) => {
                    return self
                        .handle_error(ErrorContext::Body, error, cancellation.clone())
                        .await;
                }
            };
            if eom {
                return Ok(());
            }
            if stop || cancellation.is_cancellation_requested() {
                return Ok(());
            }
        }
    }

    async fn run_pre_read(
        &mut self,
        cancellation: CancellationToken,
    ) -> AwaitOutcome<Result<bool, HttpError>> {
        if let Some(callback) = &mut self.pre_read_fn {
            AwaitOutcome::Ready(Ok(callback()))
        } else if let Some(callback) = &mut self.async_pre_read_fn {
            await_or_cancel(callback(), cancellation).await
        } else {
            AwaitOutcome::Ready(Ok(false))
        }
    }

    async fn run_header(
        &mut self,
        message: HttpMessage,
        final_headers: bool,
        eom: bool,
        cancellation: CancellationToken,
    ) -> AwaitOutcome<Result<bool, HttpError>> {
        if let Some(callback) = &mut self.header_fn {
            AwaitOutcome::Ready(Ok(callback(message, final_headers, eom)))
        } else if let Some(callback) = &mut self.async_header_fn {
            await_or_cancel(callback(message, final_headers, eom), cancellation).await
        } else {
            AwaitOutcome::Ready(Ok(false))
        }
    }

    async fn run_body(
        &mut self,
        body: BufQueue,
        eom: bool,
        cancellation: CancellationToken,
    ) -> AwaitOutcome<Result<bool, HttpError>> {
        if let Some(callback) = &mut self.body_fn {
            AwaitOutcome::Ready(Ok(callback(body, eom)))
        } else if let Some(callback) = &mut self.async_body_fn {
            await_or_cancel(callback(body, eom), cancellation).await
        } else {
            AwaitOutcome::Ready(Ok(false))
        }
    }

    async fn run_push_promise(
        &mut self,
        push: HttpPushEvent,
        eom: bool,
        cancellation: CancellationToken,
    ) -> AwaitOutcome<Result<bool, HttpError>> {
        if let Some(callback) = &mut self.promise_fn {
            AwaitOutcome::Ready(Ok(callback(push, eom)))
        } else if let Some(callback) = &mut self.async_promise_fn {
            await_or_cancel(callback(push, eom), cancellation).await
        } else {
            AwaitOutcome::Ready(Ok(false))
        }
    }

    async fn run_trailers(
        &mut self,
        trailers: HttpHeaders,
        cancellation: CancellationToken,
    ) -> AwaitOutcome<Result<bool, HttpError>> {
        if let Some(callback) = &mut self.trailer_fn {
            callback(trailers);
            AwaitOutcome::Ready(Ok(false))
        } else if let Some(callback) = &mut self.async_trailer_fn {
            match await_or_cancel(callback(trailers), cancellation).await {
                AwaitOutcome::Cancelled => AwaitOutcome::Cancelled,
                AwaitOutcome::Ready(Ok(())) => AwaitOutcome::Ready(Ok(false)),
                AwaitOutcome::Ready(Err(error)) => AwaitOutcome::Ready(Err(error)),
            }
        } else {
            self.run_body(BufQueue::new(0), true, cancellation).await
        }
    }

    async fn handle_error(
        &mut self,
        context: ErrorContext,
        error: HttpError,
        cancellation: CancellationToken,
    ) -> Result<(), HttpError> {
        if let Some(callback) = &mut self.error_fn {
            callback(context, error);
            Ok(())
        } else if let Some(callback) = &mut self.fallible_error_fn {
            callback(context, error)
        } else if let Some(callback) = &mut self.async_error_fn {
            match await_or_cancel(callback(context, error), cancellation).await {
                AwaitOutcome::Ready(result) => result,
                AwaitOutcome::Cancelled => Ok(()),
            }
        } else {
            Err(error)
        }
    }
}

enum AwaitOutcome<T> {
    Ready(T),
    Cancelled,
}

async fn await_or_cancel<T>(
    mut operation: LocalSourceFuture<T>,
    cancellation: CancellationToken,
) -> AwaitOutcome<T> {
    if cancellation.is_cancellation_requested() {
        return AwaitOutcome::Cancelled;
    }
    let baton = CancellableBaton::new();
    let mut cancelled = Box::pin(baton.wait(cancellation));
    poll_fn(|context| {
        if let Poll::Ready(value) = operation.as_mut().poll(context) {
            return Poll::Ready(AwaitOutcome::Ready(value));
        }
        match cancelled.as_mut().poll(context) {
            Poll::Ready(WaitStatus::Cancelled) => Poll::Ready(AwaitOutcome::Cancelled),
            Poll::Ready(_) | Poll::Pending => Poll::Pending,
        }
    })
    .await
}

fn source_release_error(error: SourceReleaseError) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        error.to_string().as_bytes(),
    )
}

fn missing_source_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"HTTP source reader has no source",
    )
}
