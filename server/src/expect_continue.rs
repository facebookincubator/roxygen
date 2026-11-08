//! `Expect: 100-continue` handler wrapper.

use std::cell::RefCell;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::hybrid_source::HttpHybridSource;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;

/// Not in the common-header table, so addressed by name.
const EXPECT: &str = "Expect";
const CONTINUE_EXPECTATION: &[u8] = b"100-continue";
const CONTINUE_STATUS: u16 = 100;
const EXPECTATION_FAILED_STATUS: u16 = 417;
const EXPECTATION_FAILED_BODY: &[u8] = b"Expectation Failed";

/// Injects an informational response before lazily invoking the wrapped handler.
pub struct ExpectContinueWrapperHandler {
    next_handler: HttpHandlerRef,
}

impl ExpectContinueWrapperHandler {
    pub fn new(next_handler: HttpHandlerRef) -> Self {
        Self { next_handler }
    }
}

impl HttpHandler for ExpectContinueWrapperHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let next_handler = Rc::clone(&self.next_handler);
        Box::pin(async move {
            let header_event = request_source.read_header_event().await?;
            let mut request = header_event.into_headers();
            if !request.headers().exists(EXPECT) {
                let request = hybrid_holder(request, request_source)?;
                return next_handler.handle_request(context, request).await;
            }

            let expectation = request.headers().get_single_or_empty(EXPECT);
            if !expectation.eq_ignore_ascii_case(CONTINUE_EXPECTATION) {
                let body = BufQueue::from_bytes(
                    Bytes::from_static(EXPECTATION_FAILED_BODY),
                    EXPECTATION_FAILED_BODY.len(),
                )
                .map_err(internal_error)?;
                let response =
                    HttpFixedSource::make_fixed_response(EXPECTATION_FAILED_STATUS, Some(body))
                        .map_err(internal_error)?;
                return Ok(HttpSourceHolder::new(Box::new(response)));
            }

            request.headers_mut().remove(EXPECT);
            let request = hybrid_holder(request, request_source)?;
            Ok(HttpSourceHolder::new(Box::new(
                ExpectContinueWrapperResponse::new(context, request, next_handler),
            )))
        })
    }
}

fn hybrid_holder(
    headers: HttpMessage,
    source: HttpSourceHolder,
) -> Result<HttpSourceHolder, HttpError> {
    Ok(HttpSourceHolder::new(Box::new(
        HttpHybridSource::new(headers, Some(source)).map_err(internal_error)?,
    )))
}

fn internal_error(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
}

struct WrapperState {
    context: Option<HandlerSessionContext>,
    request: Option<HttpSourceHolder>,
    next_handler: HttpHandlerRef,
    invocation: Option<LocalHandlerFuture>,
    response: Option<HttpSourceHolder>,
    response_100_written: bool,
    stopped: bool,
}

/// Response source returned for a valid `100-continue` expectation.
pub struct ExpectContinueWrapperResponse {
    state: Rc<RefCell<WrapperState>>,
}

impl ExpectContinueWrapperResponse {
    pub fn new(
        context: HandlerSessionContext,
        request: HttpSourceHolder,
        next_handler: HttpHandlerRef,
    ) -> Self {
        Self {
            state: Rc::new(RefCell::new(WrapperState {
                context: Some(context),
                request: Some(request),
                next_handler,
                invocation: None,
                response: None,
                response_100_written: false,
                stopped: false,
            })),
        }
    }

    fn ensure_response(
        state: Rc<RefCell<WrapperState>>,
    ) -> LocalSourceFuture<Result<(), HttpError>> {
        Box::pin(async move {
            let setup = {
                let mut state = state.borrow_mut();
                if state.stopped {
                    return Err(cancelled_error());
                }
                if state.response.is_some() {
                    return Ok(());
                }
                if state.invocation.is_none() {
                    Some((
                        Rc::clone(&state.next_handler),
                        state.context.take().ok_or_else(invalid_state)?,
                        state.request.take().ok_or_else(invalid_state)?,
                    ))
                } else {
                    None
                }
            };

            if let Some((handler, context, request)) = setup {
                let mut invocation = Some(handler.handle_request(context, request));
                let stopped = {
                    let mut state = state.borrow_mut();
                    if state.stopped {
                        true
                    } else if state.invocation.is_some() || state.response.is_some() {
                        return Err(invalid_state());
                    } else {
                        state.invocation = invocation.take();
                        false
                    }
                };
                if stopped {
                    drop(invocation);
                    return Err(cancelled_error());
                }
            }

            poll_fn(move |context| {
                let mut invocation = {
                    let mut state = state.borrow_mut();
                    if state.stopped {
                        state.invocation.take();
                        return Poll::Ready(Err(cancelled_error()));
                    }
                    if state.response.is_some() {
                        return Poll::Ready(Ok(()));
                    }
                    state.invocation.take().ok_or_else(invalid_state)?
                };

                match invocation.as_mut().poll(context) {
                    Poll::Pending => {
                        let mut state = state.borrow_mut();
                        if state.stopped {
                            drop(state);
                            drop(invocation);
                            Poll::Ready(Err(cancelled_error()))
                        } else if state.invocation.is_some() || state.response.is_some() {
                            Poll::Ready(Err(invalid_state()))
                        } else {
                            state.invocation = Some(invocation);
                            Poll::Pending
                        }
                    }
                    Poll::Ready(result) => {
                        let mut state = state.borrow_mut();
                        if state.stopped {
                            drop(state);
                            drop(result);
                            return Poll::Ready(Err(cancelled_error()));
                        }
                        match result {
                            Ok(response) if state.response.is_none() => {
                                state.response = Some(response);
                                Poll::Ready(Ok(()))
                            }
                            Ok(_) => Poll::Ready(Err(invalid_state())),
                            Err(error) => Poll::Ready(Err(error)),
                        }
                    }
                }
            })
            .await
        })
    }

    fn continue_event() -> Result<HttpHeaderEvent, HttpError> {
        let mut response = HttpMessage::new();
        response
            .set_status_code(CONTINUE_STATUS)
            .map_err(internal_error)?;
        response
            .set_status_message(b"Continue")
            .map_err(internal_error)?;
        response.set_http_version(1, 1);
        HttpHeaderEvent::new(response, false).map_err(internal_error)
    }
}

impl HttpSource for ExpectContinueWrapperResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return Box::pin(std::future::ready(Err(cancelled_error())));
            }
            if !state.response_100_written {
                state.response_100_written = true;
                return Box::pin(std::future::ready(Self::continue_event()));
            }
        }

        let state = Rc::clone(&self.state);
        Box::pin(async move {
            Self::ensure_response(Rc::clone(&state)).await?;
            let mut response = Some(
                state
                    .borrow_mut()
                    .response
                    .take()
                    .ok_or_else(invalid_state)?,
            );
            let read = response
                .as_mut()
                .expect("response remains detached while its read starts")
                .read_header_event();
            let stopped = {
                let mut shared = state.borrow_mut();
                if shared.stopped {
                    true
                } else if shared.response.is_some() {
                    return Err(invalid_state());
                } else {
                    shared.response = response.take();
                    false
                }
            };
            if stopped {
                response
                    .as_mut()
                    .expect("stopped response remains detached")
                    .stop_reading(None);
                drop(read);
                return Err(cancelled_error());
            }
            let result = read.await;
            if state.borrow().stopped {
                Err(cancelled_error())
            } else {
                result
            }
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let state = Rc::clone(&self.state);
        Box::pin(async move {
            Self::ensure_response(Rc::clone(&state)).await?;
            let mut response = Some(
                state
                    .borrow_mut()
                    .response
                    .take()
                    .ok_or_else(invalid_state)?,
            );
            let read = response
                .as_mut()
                .expect("response remains detached while its read starts")
                .read_body_event(maximum);
            let stopped = {
                let mut shared = state.borrow_mut();
                if shared.stopped {
                    true
                } else if shared.response.is_some() {
                    return Err(invalid_state());
                } else {
                    shared.response = response.take();
                    false
                }
            };
            if stopped {
                response
                    .as_mut()
                    .expect("stopped response remains detached")
                    .stop_reading(None);
                drop(read);
                return Err(cancelled_error());
            }
            let result = read.await;
            if state.borrow().stopped {
                Err(cancelled_error())
            } else {
                result
            }
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        let (request, response, invocation) = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return;
            }
            state.stopped = true;
            (
                state.request.take(),
                state.response.take(),
                state.invocation.take(),
            )
        };
        if let Some(mut request) = request {
            request.stop_reading(None);
        }
        if let Some(mut response) = response {
            response.stop_reading(None);
        }
        drop(invocation);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        let mut response = Some(self.state.borrow_mut().response.take()?);
        let stream_id = response
            .as_ref()
            .expect("response remains detached while its stream ID is read")
            .get_stream_id();
        let stopped = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                true
            } else {
                state.response = response.take();
                false
            }
        };
        if stopped && let Some(mut response) = response {
            response.stop_reading(None);
        }
        stream_id
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        let (mut request, mut response) = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return;
            }
            (state.request.take(), state.response.take())
        };
        if let Some(request) = request.as_mut() {
            request.set_read_timeout(timeout);
        }
        if !self.state.borrow().stopped
            && let Some(response) = response.as_mut()
        {
            response.set_read_timeout(timeout);
        }
        let stopped = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                true
            } else {
                state.request = request.take();
                state.response = response.take();
                false
            }
        };
        if stopped {
            if let Some(mut request) = request {
                request.stop_reading(None);
            }
            if let Some(mut response) = response {
                response.stop_reading(None);
            }
        }
    }
}

fn invalid_state() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"expect-continue response cannot invoke its handler",
    )
}

fn cancelled_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::CORO_CANCELLED,
        b"expect-continue response was stopped",
    )
}
