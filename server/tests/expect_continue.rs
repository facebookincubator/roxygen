use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

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
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::keep_alive::KeepAliveOwner;

use crate::expect_continue::ExpectContinueWrapperHandler;

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test future unexpectedly pending"),
    }
}

fn expect_error<T>(result: Result<T, HttpError>) -> HttpError {
    match result {
        Ok(_) => panic!("test operation unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn body(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap()
}

fn request_message(expectation: Option<&[u8]>) -> HttpMessage {
    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Post).unwrap();
    request
        .set_url(b"https://test.facebook.com/continue", false)
        .unwrap();
    request.set_http_version(1, 1);
    if let Some(expectation) = expectation {
        request
            .headers_mut()
            .add(HttpHeaderCode::Expect, expectation);
    }
    request
        .headers_mut()
        .add(HttpHeaderCode::Host, b"test.facebook.com");
    request
}

fn fixed_request(expectation: Option<&[u8]>, request_body: Option<&[u8]>) -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(HttpFixedSource::new(
        request_message(expectation),
        request_body.map(body),
    )))
}

fn fixed_response() -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_fixed_response(200, Some(body(b"MOCK BODY"))).unwrap(),
    ))
}

struct RecordingHandler {
    calls: Rc<Cell<usize>>,
    request_headers: Rc<RefCell<Vec<(Box<[u8]>, Box<[u8]>, bool)>>>,
    response_stop_count: Option<Rc<Cell<usize>>>,
}

impl HttpHandler for RecordingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.calls.set(self.calls.get() + 1);
        let request_headers = Rc::clone(&self.request_headers);
        let response_stop_count = self.response_stop_count.clone();
        Box::pin(async move {
            let request = request_source.read_header_event().await?;
            request_headers.borrow_mut().push((
                request.headers().headers().raw_get(HttpHeaderCode::Expect),
                request.headers().headers().raw_get(HttpHeaderCode::Host),
                request.eom(),
            ));
            match response_stop_count {
                Some(stop_count) => Ok(HttpSourceHolder::new(Box::new(TrackingSource::new(
                    response_message(),
                    Some(body(b"MOCK BODY")),
                    stop_count,
                )))),
                None => Ok(fixed_response()),
            }
        })
    }
}

struct TrackingSource {
    headers: Option<HttpMessage>,
    body: Option<BufQueue>,
    stop_count: Rc<Cell<usize>>,
    stop_errors: Option<Rc<RefCell<Vec<Option<HttpErrorCode>>>>>,
}

impl TrackingSource {
    fn new(headers: HttpMessage, body: Option<BufQueue>, stop_count: Rc<Cell<usize>>) -> Self {
        Self {
            headers: Some(headers),
            body,
            stop_count,
            stop_errors: None,
        }
    }

    fn with_stop_errors(mut self, stop_errors: Rc<RefCell<Vec<Option<HttpErrorCode>>>>) -> Self {
        self.stop_errors = Some(stop_errors);
        self
    }
}

impl HttpSource for TrackingSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = self.headers.take().map_or_else(
            || {
                Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"headers already read",
                ))
            },
            |headers| HttpHeaderEvent::new(headers, self.body.is_none()).map_err(event_error),
        );
        Box::pin(std::future::ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let mut data = self.body.take().unwrap_or_else(|| BufQueue::new(0));
        let chunk = data.split_at_most(maximum as usize);
        if !data.is_empty() {
            self.body = Some(data);
        }
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
            chunk,
            self.body.is_none(),
        ))))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.stop_count.set(self.stop_count.get() + 1);
        if let Some(stop_errors) = self.stop_errors.as_ref() {
            stop_errors.borrow_mut().push(error);
        }
    }
}

struct ReentrantHandler {
    target: Rc<RefCell<Option<HttpSourceHolder>>>,
}

impl HttpHandler for ReentrantHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.target
            .borrow_mut()
            .as_mut()
            .expect("test target is installed before handler invocation")
            .stop_reading(Some(HttpErrorCode::CANCEL));
        Box::pin(std::future::ready(Ok(fixed_response())))
    }
}

struct ReentrantResponseSource {
    target: Rc<RefCell<Option<HttpSourceHolder>>>,
    source: HttpFixedSource,
    stop_errors: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
}

impl HttpSource for ReentrantResponseSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.target
            .borrow_mut()
            .as_mut()
            .expect("test target is installed before response reads")
            .stop_reading(Some(HttpErrorCode::CANCEL));
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.source.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.stop_errors.borrow_mut().push(error);
        self.source.stop_reading(error);
    }
}

struct ReentrantResponseHandler {
    target: Rc<RefCell<Option<HttpSourceHolder>>>,
    stop_errors: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
}

impl HttpHandler for ReentrantResponseHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let source = ReentrantResponseSource {
            target: Rc::clone(&self.target),
            source: HttpFixedSource::make_fixed_response(200, None).unwrap(),
            stop_errors: Rc::clone(&self.stop_errors),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

fn event_error(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
}

fn response_message() -> HttpMessage {
    let mut response = HttpMessage::new();
    response.set_status_code(200).unwrap();
    response.set_status_message(b"OK").unwrap();
    response.set_http_version(1, 1);
    response
}

struct Harness {
    owner: KeepAliveOwner<()>,
    calls: Rc<Cell<usize>>,
    request_headers: Rc<RefCell<Vec<(Box<[u8]>, Box<[u8]>, bool)>>>,
    handler: ExpectContinueWrapperHandler,
}

impl Harness {
    fn new(response_stop_count: Option<Rc<Cell<usize>>>) -> Self {
        let calls = Rc::new(Cell::new(0));
        let request_headers = Rc::new(RefCell::new(Vec::new()));
        let next: HttpHandlerRef = Rc::new(RecordingHandler {
            calls: Rc::clone(&calls),
            request_headers: Rc::clone(&request_headers),
            response_stop_count,
        });
        Self {
            owner: KeepAliveOwner::new(()),
            calls,
            request_headers,
            handler: ExpectContinueWrapperHandler::new(next),
        }
    }

    fn invoke(&self, request: HttpSourceHolder) -> HttpSourceHolder {
        let context = HandlerSessionContext::acquire(&self.owner).unwrap();
        ready(self.handler.handle_request(context, request)).unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        ready(self.owner.finalize());
    }
}

#[test]
fn send_continue_response() {
    let harness = Harness::new(None);
    let mut response = harness.invoke(fixed_request(Some(b"100-CoNtInUe"), None));

    let mut interim = ready(response.read_header_event()).unwrap();
    assert_eq!(interim.headers_mut().status_code().unwrap(), 100);
    assert_eq!(interim.headers_mut().status_message().unwrap(), b"Continue");
    assert!(!interim.eom());
    assert_eq!(harness.calls.get(), 0);

    let mut final_headers = ready(response.read_header_event()).unwrap();
    assert_eq!(final_headers.headers_mut().status_code().unwrap(), 200);
    assert!(!final_headers.eom());
    assert_eq!(harness.calls.get(), 1);
    assert_eq!(
        harness.request_headers.borrow().as_slice(),
        &[(
            Box::from(&b""[..]),
            Box::from(&b"test.facebook.com"[..]),
            true
        )]
    );

    let final_body = ready(response.read_body_event(u32::MAX)).unwrap();
    assert_eq!(
        final_body
            .as_body()
            .unwrap()
            .coalesce(usize::MAX)
            .unwrap()
            .as_ref(),
        b"MOCK BODY"
    );
    assert!(final_body.eom());
}

#[test]
fn no_expectation() {
    let harness = Harness::new(None);
    let mut response = harness.invoke(fixed_request(None, None));

    let mut headers = ready(response.read_header_event()).unwrap();
    assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
    assert_eq!(harness.calls.get(), 1);
    let body = ready(response.read_body_event(u32::MAX)).unwrap();
    assert_eq!(
        body.as_body()
            .unwrap()
            .coalesce(usize::MAX)
            .unwrap()
            .as_ref(),
        b"MOCK BODY"
    );
    assert!(body.eom());
}

#[test]
fn invalid_expectation() {
    let harness = Harness::new(None);
    let mut response = harness.invoke(fixed_request(Some(b"blah"), None));

    let mut headers = ready(response.read_header_event()).unwrap();
    assert_eq!(headers.headers_mut().status_code().unwrap(), 417);
    assert_eq!(
        headers.headers_mut().status_message().unwrap(),
        b"Expectation Failed"
    );
    assert!(headers.headers().wants_keepalive());
    assert!(!headers.eom());
    let body = ready(response.read_body_event(u32::MAX)).unwrap();
    assert_eq!(
        body.as_body()
            .unwrap()
            .coalesce(usize::MAX)
            .unwrap()
            .as_ref(),
        EXPECTATION_FAILED_BODY
    );
    assert!(body.eom());
    assert_eq!(harness.calls.get(), 0);
}

const EXPECTATION_FAILED_BODY: &[u8] = b"Expectation Failed";

#[test]
fn stop_before_no_body() {
    let harness = Harness::new(None);
    let mut response = harness.invoke(fixed_request(Some(b"100-continue"), None));
    let mut interim = ready(response.read_header_event()).unwrap();
    assert_eq!(interim.headers_mut().status_code().unwrap(), 100);

    response.stop_reading(None);
    assert_eq!(harness.calls.get(), 0);
}

#[test]
fn stop_before_with_body() {
    let request_stop_count = Rc::new(Cell::new(0));
    let request_stop_errors = Rc::new(RefCell::new(Vec::new()));
    let request = HttpSourceHolder::new(Box::new(
        TrackingSource::new(
            request_message(Some(b"100-continue")),
            Some(body(b"REQUEST BODY")),
            Rc::clone(&request_stop_count),
        )
        .with_stop_errors(Rc::clone(&request_stop_errors)),
    ));
    let harness = Harness::new(None);
    let mut response = harness.invoke(request);
    let mut interim = ready(response.read_header_event()).unwrap();
    assert_eq!(interim.headers_mut().status_code().unwrap(), 100);

    response.stop_reading(Some(HttpErrorCode::CANCEL));
    assert_eq!(harness.calls.get(), 0);
    assert_eq!(request_stop_count.get(), 1);
    assert_eq!(request_stop_errors.borrow().as_slice(), &[None]);
}

#[test]
fn stop_after_invoke() {
    let response_stop_count = Rc::new(Cell::new(0));
    let harness = Harness::new(Some(Rc::clone(&response_stop_count)));
    let mut response = harness.invoke(fixed_request(Some(b"100-continue"), None));
    let mut interim = ready(response.read_header_event()).unwrap();
    assert_eq!(interim.headers_mut().status_code().unwrap(), 100);
    let mut final_headers = ready(response.read_header_event()).unwrap();
    assert_eq!(final_headers.headers_mut().status_code().unwrap(), 200);
    assert_eq!(harness.calls.get(), 1);

    response.stop_reading(None);
    assert_eq!(response_stop_count.get(), 1);
}

#[test]
fn reentrant_handler_stop_is_safe() {
    let mut owner = KeepAliveOwner::new(());
    let target = Rc::new(RefCell::new(None));
    let next: HttpHandlerRef = Rc::new(ReentrantHandler {
        target: Rc::clone(&target),
    });
    let handler = ExpectContinueWrapperHandler::new(next);
    let context = HandlerSessionContext::acquire(&owner).unwrap();
    let response =
        ready(handler.handle_request(context, fixed_request(Some(b"100-continue"), None))).unwrap();
    target.borrow_mut().replace(response);

    let interim_read = target.borrow_mut().as_mut().unwrap().read_header_event();
    let mut interim = ready(interim_read).unwrap();
    assert_eq!(interim.headers_mut().status_code().unwrap(), 100);
    let final_read = target.borrow_mut().as_mut().unwrap().read_header_event();
    let error = expect_error(ready(final_read));
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);

    target.borrow_mut().take();
    ready(owner.finalize());
}

#[test]
fn reentrant_response_stop_is_safe() {
    let mut owner = KeepAliveOwner::new(());
    let target = Rc::new(RefCell::new(None));
    let stop_errors = Rc::new(RefCell::new(Vec::new()));
    let next: HttpHandlerRef = Rc::new(ReentrantResponseHandler {
        target: Rc::clone(&target),
        stop_errors: Rc::clone(&stop_errors),
    });
    let handler = ExpectContinueWrapperHandler::new(next);
    let context = HandlerSessionContext::acquire(&owner).unwrap();
    let response =
        ready(handler.handle_request(context, fixed_request(Some(b"100-continue"), None))).unwrap();
    target.borrow_mut().replace(response);

    let interim_read = target.borrow_mut().as_mut().unwrap().read_header_event();
    ready(interim_read).unwrap();
    let final_read = target.borrow_mut().as_mut().unwrap().read_header_event();
    let error = expect_error(ready(final_read));
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
    assert_eq!(stop_errors.borrow().as_slice(), &[None]);

    target.borrow_mut().take();
    ready(owner.finalize());
}
