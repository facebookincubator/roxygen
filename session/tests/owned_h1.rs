/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Owned HTTP/1.1 downstream driver tests. Each byte comparison runs `UniplexSession` HTTP/1.1
// with the same input, the same zero-timeout `UniplexConfig` and the same scripted response,
// whose fixed `date` keeps the clock out of the comparison.

use std::sync::atomic::AtomicBool;
use std::task::Context;
use std::task::Waker;

use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::tokio_host::ManagedWorkerError;

use super::*;
use crate::DeadlineExpiry;
use crate::OwnedConnectionDeadlines;
use crate::external_host_tests::ManualHost;
use crate::external_host_tests::YieldMode;
use crate::external_host_tests::after_forced_teardown;
use crate::external_host_tests::forced_teardown_of_scope;
use crate::external_host_tests::panic_text;
use crate::external_host_tests::poll_once;
use crate::external_host_tests::run_on_host;
use crate::external_host_tests::run_on_host_with;
use crate::external_host_tests::run_on_wheel_host_with;
use crate::owned::OwnedApplication;
use crate::owned::OwnedExchange;
use crate::owned::PanicSite;
use crate::owned::deadline_timers;
use crate::owned::take_contained_panics;
use crate::owned_h1::Http1DriverConfig;
use crate::owned_h1::Http1UpgradePolicy;
use crate::owned_h1::OwnedH1Handle;
use crate::owned_h1::OwnedH1Options;
use crate::owned_h1::OwnedH1Session;
use crate::owned_h1::SignalProbe;
use crate::owned_h1::after_next_body_event_serialized;
use crate::owned_h1::codec_parses;
use crate::owned_h1::driver_turns;
use crate::owned_h1::phase_checks;
use crate::owned_h1::read_start_checks;
use crate::owned_h1::read_write_polls_and_signal_takes;
use crate::uniplex::UniplexWriteCompletion;
use crate::uniplex::UniplexWriteQueue;

const DATE: &[u8] = b"Tue, 03 Nov 2026 00:00:00 GMT";

/// The configuration both sessions share in every comparison.
fn h1_config() -> UniplexConfig {
    UniplexConfig {
        read_timeout: Duration::ZERO,
        stream_read_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        drain_request_body_on_drop: true,
        ..UniplexConfig::default()
    }
}

/// One scripted response, replayed by both the owned exchange and the Uniplex handler.
#[derive(Clone)]
struct Response {
    status: u16,
    body: Vec<u8>,
    content_length: bool,
    /// A `Content-Length` sent in place of the body's length, as a HEAD response sends one.
    declared_length: Option<usize>,
    /// A trailer section sent after the body, which then does not end the response.
    trailers: Vec<(&'static str, &'static str)>,
    /// Informational statuses sent, in order, before the request body is read.
    interim: Vec<u16>,
    /// The head is sent before the request body is read, and the body after it ends, as a
    /// tunnel's response is.
    tunnel: bool,
    /// The head accepts a WebSocket upgrade.
    websocket: bool,
    /// A suspension precedes the body, and resumes with this status on its second poll.
    suspend: Option<ResumeStatus>,
}

impl Response {
    fn fixed(status: u16, body: &[u8]) -> Self {
        Self {
            status,
            body: body.to_vec(),
            content_length: true,
            declared_length: None,
            trailers: Vec::new(),
            interim: Vec::new(),
            tunnel: false,
            websocket: false,
            suspend: None,
        }
    }

    /// Accepts a tunnel with `status`, then sends `body` once the tunnel's ingress ends.
    fn tunnel(status: u16, body: &[u8], websocket: bool) -> Self {
        Self {
            content_length: false,
            tunnel: true,
            websocket,
            ..Self::fixed(status, body)
        }
    }

    fn interim_events(&self) -> VecDeque<HttpHeaderEvent> {
        self.interim
            .iter()
            .map(|status| {
                let mut message = HttpMessage::new();
                message.set_status_code(*status).unwrap();
                message.set_http_version(1, 1);
                message.headers_mut().set(HttpHeaderCode::Date, DATE);
                HttpHeaderEvent::new(message, false).unwrap()
            })
            .collect()
    }

    /// A body-less response with `Content-Length: length`, as a response to HEAD can have.
    fn declaring(status: u16, length: usize) -> Self {
        Self {
            declared_length: Some(length),
            ..Self::fixed(status, b"")
        }
    }

    fn events(&self) -> (HttpHeaderEvent, VecDeque<HttpBodyEvent>) {
        let mut message = HttpMessage::new();
        message.set_status_code(self.status).unwrap();
        message.set_http_version(1, 1);
        if self.content_length {
            message.headers_mut().set(
                HttpHeaderCode::ContentLength,
                self.declared_length
                    .unwrap_or(self.body.len())
                    .to_string()
                    .as_bytes(),
            );
        }
        message.headers_mut().set(HttpHeaderCode::Date, DATE);
        if self.websocket {
            message.set_egress_websocket_upgrade();
        }
        let header_eom = self.body.is_empty() && self.trailers.is_empty();
        let header = HttpHeaderEvent::new(message, header_eom).unwrap();
        let mut body = VecDeque::new();
        if let Some(status) = self.suspend {
            body.push_back(HttpBodyEvent::suspend(Box::pin(ResumeAfterWake {
                status,
                polls: 0,
            })));
        }
        if !self.body.is_empty() {
            body.push_back(HttpBodyEvent::body(
                queue(&self.body),
                self.trailers.is_empty(),
            ));
        }
        if !self.trailers.is_empty() {
            let mut trailers = HttpHeaders::new();
            for (name, value) in &self.trailers {
                trailers.add(name.as_bytes(), value.as_bytes());
            }
            body.push_back(HttpBodyEvent::trailers(trailers));
        }
        (header, body)
    }
}

/// Resumes with `status` on its second poll, after waking its first poller, as a source's
/// suspension resumes once its producer pushes. It must be awaited to completion.
struct ResumeAfterWake {
    status: ResumeStatus,
    polls: u8,
}

impl Future for ResumeAfterWake {
    type Output = ResumeStatus;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<ResumeStatus> {
        self.polls += 1;
        if self.polls > 1 {
            return Poll::Ready(self.status);
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for ResumeAfterWake {
    fn drop(&mut self) {
        assert!(
            self.polls > 1 || std::thread::panicking(),
            "the body was read again before its suspension resumed"
        );
    }
}

/// A suspension the test releases from outside the driver, and what the driver did with it.
#[derive(Default)]
struct ResumeProbe {
    /// The status the resume returns once set; it pends until then.
    status: Cell<Option<ResumeStatus>>,
    /// The waker of the resume's last pending poll.
    waker: RefCell<Option<Waker>>,
    polls: Cell<usize>,
    drops: Cell<usize>,
    /// Cancels the session from the named callback: `resume_poll` or `resume_drop`.
    cancel_in: CancelIn,
}

impl ResumeProbe {
    /// Resumes the suspension with `status`, waking its pending poll.
    fn release(&self, status: ResumeStatus) {
        self.status.set(Some(status));
        self.waker.borrow_mut().take().unwrap().wake();
    }
}

/// The resume of a suspension that `ResumeProbe` releases.
struct BodyResume(Rc<ResumeProbe>);

impl Future for BodyResume {
    type Output = ResumeStatus;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<ResumeStatus> {
        self.0.polls.set(self.0.polls.get() + 1);
        cancel_from(&self.0.cancel_in, "resume_poll");
        match self.0.status.get() {
            Some(status) => Poll::Ready(status),
            None => {
                *self.0.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for BodyResume {
    fn drop(&mut self) {
        self.0.drops.set(self.0.drops.get() + 1);
        cancel_from(&self.0.cancel_in, "resume_drop");
    }
}

/// What the application of one exchange observed.
#[derive(Default)]
struct H1Probe {
    method: RefCell<Option<HttpMethod>>,
    /// The request method as sent (`HttpMessage::request_method`).
    request_method: RefCell<Vec<u8>>,
    url: RefCell<Vec<u8>>,
    /// The request head's fields, in order.
    fields: RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
    /// The error a request body read returned.
    request_error: Cell<Option<HttpErrorCode>>,
    request_eom: Cell<bool>,
    request_body: RefCell<Vec<u8>>,
    request_finished: Cell<bool>,
    /// Each request trailers event: its fields, and whether it carried EOM.
    request_trailers: RefCell<Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>>,
    response_complete: Cell<usize>,
    completed: Cell<usize>,
    cancelled: RefCell<Vec<(HttpErrorCode, bool)>>,
    dropped: Cell<usize>,
    headers_encoded: RefCell<Vec<u32>>,
    headers_polled: Cell<usize>,
    /// Each byte event registered on the response: `None` if it fired, else the cancel code.
    byte_events: RefCell<Vec<Option<HttpErrorCode>>>,
    /// The error of each `stop_reading` call on the response source `ScriptHandler` returns.
    response_stopped: RefCell<Vec<Option<HttpErrorCode>>>,
    /// The application's `cancel_in`, for the `byte_event` callback.
    cancel_in: CancelIn,
    /// Whether the session's handshake had completed when `request` ran, recorded when
    /// `H1App::session` is set.
    handshake_at_request: Cell<Option<bool>>,
}

impl HttpByteEventCallback for H1Probe {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.byte_events.borrow_mut().push(None);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.byte_events.borrow_mut().push(Some(error.code()));
        cancel_from(&self.cancel_in, "byte_event");
    }
}

thread_local! {
    /// Exchange lifecycle events on this test thread, in order: (event, exchange ordinal).
    static H1_EVENTS: RefCell<Vec<(&'static str, usize)>> = const { RefCell::new(Vec::new()) };
}

fn note(event: &'static str, ordinal: usize) {
    H1_EVENTS.with(|events| events.borrow_mut().push((event, ordinal)));
}

/// Takes this thread's exchange lifecycle events.
fn take_events() -> Vec<(&'static str, usize)> {
    H1_EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

struct H1App {
    responses: Vec<Response>,
    probes: Rc<RefCell<Vec<Rc<H1Probe>>>>,
    /// Each exchange starts reading its request body only once this is notified.
    body_gate: Option<Rc<tokio::sync::Notify>>,
    /// The `maximum` of each request body read.
    body_read_size: u32,
    /// Each exchange drops its request body unread and responds immediately.
    respond_early: bool,
    /// Each response head registers a callback for its encoded size, and every response event
    /// registers a transport-write byte event.
    instrument: bool,
    /// Once set, each exchange cancels the session through this handle from the named
    /// callback: `new_exchange`, `request`, `poll_headers`, `poll_body`, `response_complete`,
    /// `complete` or (instrumented) `byte_event` or `headers_encoded`.
    cancel_in: CancelIn,
    /// Once set, each exchange drains the session through this handle from the named callback:
    /// `request`, `poll_headers` or `complete`.
    drain_in: CancelIn,
    /// The first exchange panics at this point: in `poll_headers`, or at its retirement, in
    /// `complete` or `drop`.
    panic_in: Option<&'static str>,
    /// Each exchange with a request body keeps it unread and stops it with this code, at this
    /// point.
    stop_request: Option<(HttpErrorCode, StopAt)>,
    /// Where each exchange keeps its request body for `StopAt::Script`.
    script_held: Rc<RefCell<Option<HttpSourceHolder>>>,
    /// Each response body starts with a suspension that this probe releases.
    response_resume: Option<Rc<ResumeProbe>>,
    /// The handle of the session serving this application, which `new_with` supplies.
    session: Option<OwnedH1Handle>,
}

/// Where an exchange with `H1App::stop_request` stops its request body.
#[derive(Clone, Copy, Debug)]
enum StopAt {
    /// In `poll_headers`, just before it returns the response head.
    Head,
    /// In `poll_body`, once `H1App::body_gate` is notified. That poll returns `Pending`, so the
    /// driver observes the stop before the response's next event.
    Body,
    /// In `poll_body`, once `H1App::body_gate` is notified, in the poll that returns the
    /// response's last event.
    Last,
    /// In the cancellation callback of a byte event registered on the response's last event,
    /// which the driver cancels before it serializes that event.
    Refusal,
    /// By the test, outside the driver's poll, from `H1App::script_held`. The response waits
    /// for `H1App::body_gate`.
    Script,
}

/// Stops a held request body from a byte event's cancellation callback.
struct StopOnRefusal {
    code: HttpErrorCode,
    held: RefCell<Option<HttpSourceHolder>>,
}

impl HttpByteEventCallback for StopOnRefusal {
    fn on_byte_event(&self, _event: HttpByteEvent) {}

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
        let held = self.held.borrow_mut().take();
        if let Some(mut body) = held {
            body.stop_reading(Some(self.code));
        }
    }
}

type CancelIn = Rc<RefCell<Option<(&'static str, OwnedH1Handle)>>>;

/// The handle in `slot` if `callback` is the one it names.
fn handle_for(slot: &CancelIn, callback: &str) -> Option<OwnedH1Handle> {
    slot.borrow()
        .as_ref()
        .filter(|(name, _)| *name == callback)
        .map(|(_, handle)| handle.clone())
}

/// Cancels the session if `callback` is the one `cancel_in` names.
fn cancel_from(cancel_in: &CancelIn, callback: &str) {
    if let Some(handle) = handle_for(cancel_in, callback) {
        handle.cancel();
    }
}

/// Drains the session if `callback` is the one `drain_in` names.
fn drain_from(drain_in: &CancelIn, callback: &str) {
    if let Some(handle) = handle_for(drain_in, callback) {
        assert!(handle.initiate_drain().unwrap());
    }
}

/// A sendable copy of one exchange's observations, taken after the run.
#[derive(Clone, Debug)]
struct Observed {
    method: Option<HttpMethod>,
    request_method: Vec<u8>,
    url: Vec<u8>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    request_error: Option<HttpErrorCode>,
    request_eom: bool,
    request_body: Vec<u8>,
    request_finished: bool,
    request_trailers: Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>,
    response_complete: usize,
    completed: usize,
    cancelled: Vec<(HttpErrorCode, bool)>,
    dropped: usize,
    headers_encoded: Vec<u32>,
    headers_polled: usize,
    byte_events: Vec<Option<HttpErrorCode>>,
    response_stopped: Vec<Option<HttpErrorCode>>,
}

impl H1Probe {
    fn observed(&self) -> Observed {
        Observed {
            method: *self.method.borrow(),
            request_method: self.request_method.borrow().clone(),
            url: self.url.borrow().clone(),
            fields: self.fields.borrow().clone(),
            request_error: self.request_error.get(),
            request_eom: self.request_eom.get(),
            request_body: self.request_body.borrow().clone(),
            request_finished: self.request_finished.get(),
            request_trailers: self.request_trailers.borrow().clone(),
            response_complete: self.response_complete.get(),
            completed: self.completed.get(),
            cancelled: self.cancelled.borrow().clone(),
            dropped: self.dropped.get(),
            headers_encoded: self.headers_encoded.borrow().clone(),
            headers_polled: self.headers_polled.get(),
            byte_events: self.byte_events.borrow().clone(),
            response_stopped: self.response_stopped.borrow().clone(),
        }
    }
}

/// Records a request head as either application receives it.
fn record_head(probe: &H1Probe, head: &mut HttpMessage, eom: bool) {
    *probe.method.borrow_mut() = head.method().unwrap();
    *probe.request_method.borrow_mut() = head.request_method().unwrap().as_bytes().to_vec();
    *probe.url.borrow_mut() = head.url().unwrap().to_vec();
    *probe.fields.borrow_mut() = head
        .headers()
        .iter()
        .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
        .collect();
    probe.request_eom.set(eom);
    probe.request_finished.set(eom);
}

/// Reads a request body to its end as either application does, recording each event.
async fn read_request_body(
    probe: &H1Probe,
    body: &mut HttpSourceHolder,
    maximum: u32,
) -> Result<(), HttpError> {
    loop {
        let event = match body.read_body_event(maximum).await {
            Ok(event) => event,
            Err(error) => {
                probe.request_error.set(Some(error.code()));
                return Err(error);
            }
        };
        if let Some(bytes) = event.as_body() {
            let length = bytes.chain_length();
            probe
                .request_body
                .borrow_mut()
                .extend_from_slice(&bytes.coalesce(length).unwrap());
        }
        if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
            let fields = trailers
                .iter()
                .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
                .collect();
            probe
                .request_trailers
                .borrow_mut()
                .push((fields, event.eom()));
        }
        if event.eom() {
            probe.request_finished.set(true);
            return Ok(());
        }
    }
}

impl H1App {
    fn new(responses: Vec<Response>) -> (Self, Rc<RefCell<Vec<Rc<H1Probe>>>>) {
        let probes = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                responses,
                probes: Rc::clone(&probes),
                body_gate: None,
                body_read_size: u32::MAX,
                respond_early: false,
                instrument: false,
                cancel_in: Rc::default(),
                drain_in: Rc::default(),
                panic_in: None,
                stop_request: None,
                script_held: Rc::default(),
                response_resume: None,
                session: None,
            },
            probes,
        )
    }
}

struct H1Exchange {
    ordinal: usize,
    body_gate: Option<Rc<tokio::sync::Notify>>,
    body_read_size: u32,
    respond_early: bool,
    tunnel: bool,
    cancel_in: CancelIn,
    drain_in: CancelIn,
    panic_in: Option<&'static str>,
    stop_request: Option<(HttpErrorCode, StopAt)>,
    /// The request body kept unread until `stop_request` stops it.
    held: Option<HttpSourceHolder>,
    /// Holds the request body instead of `held` for `StopAt::Refusal`.
    refusal: Option<Rc<StopOnRefusal>>,
    /// Holds the request body instead of `held` for `StopAt::Script`.
    script_held: Rc<RefCell<Option<HttpSourceHolder>>>,
    probe: Rc<H1Probe>,
    upload: Option<LocalSourceFuture<Result<(), HttpError>>>,
    interim: VecDeque<HttpHeaderEvent>,
    header: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
    session: Option<OwnedH1Handle>,
}

impl OwnedApplication for H1App {
    type Exchange = H1Exchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> H1Exchange {
        let probe = Rc::new(H1Probe {
            cancel_in: Rc::clone(&self.cancel_in),
            ..H1Probe::default()
        });
        let ordinal = self.probes.borrow().len();
        self.probes.borrow_mut().push(Rc::clone(&probe));
        let response = &self.responses[ordinal.min(self.responses.len() - 1)];
        let (mut header, mut body) = response.events();
        if let Some(resume) = &self.response_resume {
            body.push_front(HttpBodyEvent::suspend(Box::pin(BodyResume(Rc::clone(
                resume,
            )))));
        }
        if self.instrument {
            let encoded = Rc::clone(&probe);
            let cancel_in = Rc::clone(&self.cancel_in);
            header.set_egress_headers_callback(move |size| {
                encoded.headers_encoded.borrow_mut().push(size.compressed);
                cancel_from(&cancel_in, "headers_encoded");
            });
            let write = ByteEventType::TransportWrite.bit();
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(None, write, &probe));
            for event in &mut body {
                event
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(None, write, &probe));
            }
        }
        let refusal = match self.stop_request {
            Some((code, StopAt::Refusal)) => {
                let stop = Rc::new(StopOnRefusal {
                    code,
                    held: RefCell::default(),
                });
                body.back_mut()
                    .expect("the response has a last event")
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &stop,
                    ));
                Some(stop)
            }
            _ => None,
        };
        note("new", ordinal);
        cancel_from(&self.cancel_in, "new_exchange");
        H1Exchange {
            ordinal,
            body_gate: self.body_gate.clone(),
            body_read_size: self.body_read_size,
            respond_early: self.respond_early,
            tunnel: response.tunnel,
            cancel_in: Rc::clone(&self.cancel_in),
            drain_in: Rc::clone(&self.drain_in),
            panic_in: self.panic_in.filter(|_| ordinal == 0),
            stop_request: self.stop_request,
            held: None,
            refusal,
            script_held: Rc::clone(&self.script_held),
            probe,
            upload: None,
            interim: response.interim_events(),
            header: Some(header),
            body,
            session: self.session.clone(),
        }
    }
}

impl H1Exchange {
    /// Polls the request body read to its end; `Pending` while it is still being read.
    fn poll_upload(&mut self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
        let Some(upload) = &mut self.upload else {
            return Poll::Ready(Ok(()));
        };
        let result = std::task::ready!(upload.as_mut().poll(context));
        self.upload.take();
        Poll::Ready(result)
    }

    /// Stops the held request body with `code`.
    fn stop_held(&mut self, code: HttpErrorCode) {
        if let Some(mut body) = self.held.take() {
            body.stop_reading(Some(code));
        }
    }
}

impl OwnedExchange for H1Exchange {
    fn request(&mut self, mut head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        if let Some(session) = &self.session {
            self.probe
                .handshake_at_request
                .set(Some(session.handshake_complete()));
        }
        record_head(&self.probe, &mut head, eom);
        cancel_from(&self.cancel_in, "request");
        drain_from(&self.drain_in, "request");
        let mut body = body.into_source_holder();
        if eom || self.respond_early {
            return;
        }
        if let Some((_, at)) = self.stop_request {
            match (&self.refusal, at) {
                (Some(stop), _) => *stop.held.borrow_mut() = Some(body),
                (None, StopAt::Script) => {
                    *self.script_held.borrow_mut() = Some(body);
                    let gate = self
                        .body_gate
                        .clone()
                        .expect("a script stop releases the response");
                    self.upload = Some(Box::pin(async move {
                        gate.notified().await;
                        Ok(())
                    }));
                }
                (None, _) => self.held = Some(body),
            }
            return;
        }
        let probe = Rc::clone(&self.probe);
        let gate = self.body_gate.clone();
        let maximum = self.body_read_size;
        self.upload = Some(Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            read_request_body(&probe, &mut body, maximum).await
        }));
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if self.panic_in == Some("poll_headers") {
            panic!("exchange panicked polling headers");
        }
        if let Some(interim) = self.interim.pop_front() {
            return Poll::Ready(Ok(interim));
        }
        if !self.tunnel {
            std::task::ready!(self.poll_upload(context))?;
        }
        if let Some((code, StopAt::Head)) = self.stop_request {
            self.stop_held(code);
        }
        self.probe
            .headers_polled
            .set(self.probe.headers_polled.get() + 1);
        cancel_from(&self.cancel_in, "poll_headers");
        drain_from(&self.drain_in, "poll_headers");
        Poll::Ready(Ok(self
            .header
            .take()
            .expect("the response head is polled once")))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if let Some((code, StopAt::Body)) = self.stop_request
            && self.held.is_some()
        {
            if self.upload.is_none()
                && let Some(gate) = self.body_gate.clone()
            {
                self.upload = Some(Box::pin(async move {
                    gate.notified().await;
                    Ok(())
                }));
            }
            std::task::ready!(self.poll_upload(context))?;
            self.stop_held(code);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        if let Some((code, StopAt::Last)) = self.stop_request
            && self.held.is_some()
            && self.body.len() == 1
        {
            if self.upload.is_none()
                && let Some(gate) = self.body_gate.clone()
            {
                self.upload = Some(Box::pin(async move {
                    gate.notified().await;
                    Ok(())
                }));
            }
            std::task::ready!(self.poll_upload(context))?;
            self.stop_held(code);
        }
        std::task::ready!(self.poll_upload(context))?;
        cancel_from(&self.cancel_in, "poll_body");
        assert_ne!(
            maximum, 0,
            "the body is polled only with room for some of it"
        );
        let event = self
            .body
            .pop_front()
            .expect("the body is not polled after its terminal event");
        let maximum = maximum as usize;
        let oversized = event
            .as_body()
            .filter(|bytes| bytes.chain_length() > maximum)
            .map(|bytes| bytes.coalesce(bytes.chain_length()).unwrap().to_vec());
        let Some(bytes) = oversized else {
            return Poll::Ready(Ok(event));
        };
        // The contract allows at most `maximum` bytes per poll. The rest goes out on later polls.
        self.body
            .push_front(HttpBodyEvent::body(queue(&bytes[maximum..]), event.eom()));
        Poll::Ready(Ok(HttpBodyEvent::body(queue(&bytes[..maximum]), false)))
    }

    fn cancel(&mut self, error: &HttpError, peer_reset: bool) {
        self.probe
            .cancelled
            .borrow_mut()
            .push((error.code(), peer_reset));
    }

    fn response_complete(&mut self) {
        self.probe
            .response_complete
            .set(self.probe.response_complete.get() + 1);
        cancel_from(&self.cancel_in, "response_complete");
    }

    fn complete(self) {
        note("complete", self.ordinal);
        self.probe.completed.set(self.probe.completed.get() + 1);
        cancel_from(&self.cancel_in, "complete");
        drain_from(&self.drain_in, "complete");
        if self.panic_in == Some("complete") {
            panic!("exchange panicked at retirement");
        }
    }
}

impl Drop for H1Exchange {
    fn drop(&mut self) {
        note("drop", self.ordinal);
        self.probe.dropped.set(self.probe.dropped.get() + 1);
        if self.panic_in == Some("drop") && !std::thread::panicking() {
            panic!("exchange panicked at retirement");
        }
    }
}

/// The Uniplex side of a comparison: replays the script in order, reading each request as
/// `H1Exchange` does and recording what it receives.
struct ScriptHandler {
    responses: Vec<Response>,
    served: Cell<usize>,
    /// The `maximum` of each request body read.
    body_read_size: u32,
    /// Responds without reading the request body, dropping it.
    respond_early: bool,
    probes: Rc<RefCell<Vec<Rc<H1Probe>>>>,
}

impl ScriptHandler {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            responses,
            served: Cell::new(0),
            body_read_size: u32::MAX,
            respond_early: false,
            probes: Rc::default(),
        }
    }
}

impl HttpHandler for ScriptHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.served.replace(self.served.get() + 1);
        let response = &self.responses[ordinal.min(self.responses.len() - 1)];
        let (header, body) = response.events();
        let interim = response.interim_events();
        let tunnel = response.tunnel;
        let body_read_size = self.body_read_size;
        let respond_early = self.respond_early;
        let probe = Rc::new(H1Probe::default());
        self.probes.borrow_mut().push(Rc::clone(&probe));
        Box::pin(async move {
            let mut head = match request_source.read_header_event().await {
                Ok(head) => head,
                Err(error) => {
                    probe.request_error.set(Some(error.code()));
                    return Err(error);
                }
            };
            let eom = head.eom();
            record_head(&probe, head.headers_mut(), eom);
            if eom || respond_early {
                return Ok(HttpSourceHolder::new(Box::new(StopProbe {
                    inner: EventSource {
                        header: Some(header),
                        body,
                    },
                    probe,
                })));
            }
            if interim.is_empty() && !tunnel {
                read_request_body(&probe, &mut request_source, body_read_size).await?;
                return Ok(HttpSourceHolder::new(Box::new(StopProbe {
                    inner: EventSource {
                        header: Some(header),
                        body,
                    },
                    probe,
                })));
            }
            Ok(HttpSourceHolder::new(Box::new(ScriptSource {
                request: Some(request_source),
                probe,
                interim,
                tunnel,
                header: Some(header),
                body,
            })))
        })
    }
}

/// Sends its informational heads immediately, then reads the request body to its end before
/// the final head (or, for a tunnel, before the body), as `H1Exchange` does.
struct ScriptSource {
    request: Option<HttpSourceHolder>,
    probe: Rc<H1Probe>,
    interim: VecDeque<HttpHeaderEvent>,
    tunnel: bool,
    header: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
}

impl ScriptSource {
    /// Reads the request body to its end, then yields `then`.
    fn after_request<T: 'static>(
        &mut self,
        then: Result<T, HttpError>,
    ) -> LocalSourceFuture<Result<T, HttpError>> {
        let request = self.request.take();
        let probe = Rc::clone(&self.probe);
        Box::pin(async move {
            if let Some(mut request) = request {
                read_request_body(&probe, &mut request, u32::MAX).await?;
            }
            then
        })
    }
}

impl HttpSource for ScriptSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if let Some(interim) = self.interim.pop_front() {
            return Box::pin(std::future::ready(Ok(interim)));
        }
        let header = self.header.take().expect("the final head is read once");
        if self.tunnel {
            return Box::pin(std::future::ready(Ok(header)));
        }
        self.after_request(Ok(header))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let event = self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response body exhausted")
        });
        self.after_request(event)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.response_stopped.borrow_mut().push(error);
        if let Some(request) = &mut self.request {
            request.stop_reading(error);
        }
    }
}

/// An `EventSource` that records each `stop_reading` call in its probe.
struct StopProbe {
    inner: EventSource,
    probe: Rc<H1Probe>,
}

impl HttpSource for StopProbe {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.response_stopped.borrow_mut().push(error);
        self.inner.stop_reading(error);
    }
}

struct Wire {
    output: Vec<u8>,
    /// The length of each write, in order.
    writes: Vec<usize>,
    close: CloseKind,
    error: Option<HttpErrorCode>,
}

fn wire_of(
    state: &Rc<RefCell<TransportState>>,
    result: Result<crate::uniplex::UniplexRunSummary, HttpError>,
) -> Wire {
    let state = state.borrow();
    Wire {
        output: state.writes.concat(),
        writes: state.writes.iter().map(Vec::len).collect(),
        close: state.close,
        error: result.err().map(|error| error.code()),
    }
}

fn owned_options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH1Options {
    OwnedH1Options {
        context,
        transport,
        io: h1_config(),
        http1: Http1DriverConfig::default(),
    }
}

/// Serves `reads` with the owned driver on a `ManualHost`.
fn owned_wire(reads: Vec<ReadAction>, responses: Vec<Response>) -> (Wire, Vec<Observed>) {
    owned_wire_with(reads, responses, false)
}

/// As `owned_wire`; `instrument` sets `H1App::instrument`.
fn owned_wire_with(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
    instrument: bool,
) -> (Wire, Vec<Observed>) {
    owned_wire_configured(reads, responses, instrument, Http1DriverConfig::default())
}

/// As `owned_wire_with`, with the driver's HTTP/1.1 configuration `http1`.
fn owned_wire_configured(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
    instrument: bool,
    http1: Http1DriverConfig,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (mut application, probes) = H1App::new(responses);
            application.instrument = instrument;
            let mut options = owned_options(context, transport);
            options.http1 = http1;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let result = session.run().await;
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// Serves `reads` with `UniplexSession` HTTP/1.1 on a `ManualHost`.
fn uniplex_wire(reads: Vec<ReadAction>, responses: Vec<Response>) -> Wire {
    uniplex_wire_observed(reads, responses).0
}

/// As `uniplex_wire`, with what the handler observed of each request.
fn uniplex_wire_observed(
    reads: Vec<ReadAction>,
    responses: Vec<Response>,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            let result = session.run().await;
            let observed = handler
                .probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

#[test]
fn owned_h1_serves_a_get_on_a_manual_host() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert!(
        owned.output.ends_with(b"\r\n\r\nhello"),
        "{:?}",
        owned.output
    );
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert_eq!(probe.method, Some(HttpMethod::Get));
    assert_eq!(probe.url, b"/");
    assert!(probe.request_eom);
    assert_eq!(probe.response_complete, 1);
    assert_eq!(probe.completed, 1);
    assert_eq!(probe.dropped, 1);
    assert!(probe.cancelled.is_empty());
}

/// After a read that drained the transport, the driver polls its reader again only once the
/// reader's waker fires. Each read is polled once, after one poll that waited for the first
/// bytes, and each request gets its response on the kept-alive connection.
#[test]
fn owned_h1_polls_a_drained_reader_once_per_read() {
    let request = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
    let reads = move || {
        vec![
            ReadAction::Data(request.to_vec()),
            ReadAction::Data(request.to_vec()),
            ReadAction::Data(request.to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![
        Response::fixed(200, b"one"),
        Response::fixed(200, b"two"),
        Response::fixed(200, b"six"),
    ];
    let (wire, counts) = run_on_host(move |context| {
        Box::pin(async move {
            let (scripted, state) = ScriptedTransport::new(reads());
            let (transport, gate) = GatedReaderTransport::new(scripted);
            let (application, _probes) = H1App::new(responses);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let turns = driver_turns();
            let (result, counts) = GatedReaderTransport::run_gated(&gate, session.run()).await;
            (wire_of(&state, result), (counts, driver_turns() - turns))
        })
    });
    let (counts, turns) = counts;
    assert_eq!(
        counts,
        (5, 4),
        "(polls, reads): four reads, each polled once, after one poll that waited",
    );
    assert_eq!(
        turns, 15,
        "a drained reader makes no turn due; counting its read start as due would add an empty \
         turn after each drained read (21)",
    );
    let response = |body: &str| {
        format!(
            "HTTP/1.1 200 \r\nDate: Tue, 03 Nov 2026 00:00:00 GMT\r\nConnection: keep-alive\r\n\
             Content-Length: 3\r\n\r\n{body}"
        )
    };
    assert_eq!(
        String::from_utf8_lossy(&wire.output),
        [response("one"), response("two"), response("six")].concat(),
    );
    assert_eq!(wire.close, CloseKind::ShutdownWrite);
    assert_eq!(wire.error, None);
}

/// Serves a keep-alive GET, read in one read that drains the transport, with a chunked response
/// whose body events the test pushes while the reader stays parked (waiting for its waker, with
/// no read started). For each of three pushed events, returns the driver turns, the read starts
/// evaluated, the read and write-completion phases entered, and the wake signals taken. Also
/// returns the wire once the response has ended and `end` has ended the connection.
fn parked_h1(end: ParkedEnd) -> (Vec<(usize, usize, usize, usize, usize)>, Wire) {
    run_on_host(move |context| {
        Box::pin(async move {
            let mut reads = vec![ReadAction::Data(KEEP_ALIVE_GET.to_vec())];
            match end {
                ParkedEnd::PeerEof => reads.push(ReadAction::Eof),
                ParkedEnd::ReadError(code) => reads.push(ReadAction::Error(code)),
                ParkedEnd::Close | ParkedEnd::Cancel => {}
            }
            let (scripted, state) = ScriptedTransport::new(reads);
            let (transport, gate) = GatedReaderTransport::new(scripted);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            // A lost wake stalls the manual host, which then panics.
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert!(gate.waiting(), "the first read waits for the request");
            gate.open_one();
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert!(
                gate.waiting(),
                "the request's read drained the transport and parked the reader"
            );
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            let counts = [b"one".as_slice(), b"two", b"three"]
                .map(|chunk| {
                    let before = (
                        driver_turns(),
                        read_start_checks(),
                        read_write_polls_and_signal_takes(),
                    );
                    body.push(HttpBodyEvent::body(queue(chunk), false));
                    assert!(run.as_mut().poll(&mut poll).is_pending());
                    let (reads, writes, takes) = read_write_polls_and_signal_takes();
                    (
                        driver_turns() - before.0,
                        read_start_checks() - before.1,
                        reads - before.2.0,
                        writes - before.2.1,
                        takes - before.2.2,
                    )
                })
                .to_vec();
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(run.as_mut().poll(&mut poll).is_pending());
            match end {
                ParkedEnd::PeerEof | ParkedEnd::ReadError(_) => gate.open_one(),
                ParkedEnd::Close => assert!(handle.initiate_drain().unwrap()),
                ParkedEnd::Cancel => handle.cancel(),
            }
            let result = run.await;
            (counts, wire_of(&state, result))
        })
    })
}

/// While a drained reader waits for its waker ("parked"), each driver turn leaves the read
/// phase without evaluating a read start. Without that fast exit, each pushed body event's turn
/// would evaluate one.
#[test]
fn owned_h1_a_parked_reader_evaluates_no_read_start_while_a_response_body_is_pushed() {
    let (counts, wire) = parked_h1(ParkedEnd::Close);
    let counts: Vec<_> = counts
        .iter()
        .map(|&(turns, starts, ..)| (turns, starts))
        .collect();
    assert_eq!(
        counts,
        [(1, 0); 3],
        "(driver turns, read starts evaluated) per pushed body event"
    );
    assert!(
        wire.output
            .ends_with(b"3\r\none\r\n3\r\ntwo\r\n5\r\nthree\r\n3\r\nend\r\n0\r\n\r\n"),
        "{:?}",
        String::from_utf8_lossy(&wire.output)
    );
    assert_eq!(wire.error, None);
}

/// With the reader parked, as on an owned TCP connection between requests, each pushed body
/// event takes one turn. That turn enters the read phase once, where the parked reader ends it
/// after two loads. It enters the write-completion phase twice: once in the turn, when no write
/// is ready, and once after the event's write starts, to take its completion. The three signal
/// takes are the exchange's and the write's in the turn, and the write's after it. The body's
/// second poll clears the exchange's signal instead of taking it. A second turn would add one
/// read phase, one write-completion phase and two signal takes.
#[test]
fn owned_h1_a_pushed_body_event_enters_the_read_phase_once_and_the_write_phase_twice() {
    let (counts, _) = parked_h1(ParkedEnd::Close);
    let counts: Vec<_> = counts
        .iter()
        .map(|&(_, _, reads, writes, takes)| (reads, writes, takes))
        .collect();
    assert_eq!(
        counts,
        [(1, 2, 3); 3],
        "(read phases, write-completion phases, signal takes) per pushed body event"
    );
}

/// A parked reader still ends. The next read's EOF or failure wakes it, and the driver's own
/// idle close and abort (`close_idle`, `reset_transport`) mark its read wake in the same call.
#[test]
fn owned_h1_a_parked_reader_still_ends_on_its_wake_close_and_abort() {
    for (end, close, error) in [
        (ParkedEnd::PeerEof, CloseKind::ShutdownWrite, None),
        (
            ParkedEnd::ReadError(HttpErrorCode::TRANSPORT_READ_ERROR),
            CloseKind::Reset,
            Some(HttpErrorCode::TRANSPORT_READ_ERROR),
        ),
        (
            ParkedEnd::ReadError(HttpErrorCode::DROPPED),
            CloseKind::Reset,
            Some(HttpErrorCode::DROPPED),
        ),
        (ParkedEnd::Close, CloseKind::ShutdownWrite, None),
        (
            ParkedEnd::Cancel,
            CloseKind::Reset,
            Some(HttpErrorCode::CORO_CANCELLED),
        ),
    ] {
        let (_, wire) = parked_h1(end);
        assert!(
            wire.output.ends_with(b"3\r\nend\r\n0\r\n\r\n"),
            "{end:?}: the whole response is written first: {:?}",
            String::from_utf8_lossy(&wire.output)
        );
        assert_eq!((wire.close, wire.error), (close, error), "{end:?}");
    }
}

/// The driver reads through the transport's reader when it offers one, and writes the same
/// wire as when it reads through `read`.
#[test]
fn owned_h1_reads_through_the_transport_reader() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (direct, _) = owned_wire(reads(), responses.clone());
    let (wire, counts) = run_on_host(move |context| {
        Box::pin(async move {
            let (scripted, state) = ScriptedTransport::new(reads());
            let (transport, counts) = ReaderTransport::new(scripted);
            let (application, _probes) = H1App::new(responses);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let result = session.run().await;
            (
                wire_of(&state, result),
                (counts.direct.get(), counts.reader.get()),
            )
        })
    });
    assert_eq!(
        counts,
        (0, 2),
        "the request and the EOF are read through the reader"
    );
    assert_eq!(
        String::from_utf8_lossy(&wire.output),
        String::from_utf8_lossy(&direct.output)
    );
    assert_eq!(wire.close, direct.close);
    assert_eq!(wire.error, direct.error);
}

/// `count` keep-alive GETs in one read; the last one carries `Connection: close`.
fn pipelined_gets(count: usize) -> Vec<u8> {
    (0..count)
        .flat_map(|index| {
            let close = if index + 1 == count {
                "Connection: close\r\n"
            } else {
                ""
            };
            format!("GET /{index} HTTP/1.1\r\nHost: a\r\n{close}\r\n").into_bytes()
        })
        .collect()
}

#[test]
fn owned_h1_spawns_no_task_per_request() {
    let (spawned, completed) = run_on_host_with(|host, context| {
        Box::pin(async move {
            let (transport, _state) =
                ScriptedTransport::new([ReadAction::Data(pipelined_gets(100))]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let before = host.spawns();
            session.run().await.unwrap();
            let completed: usize = probes
                .borrow()
                .iter()
                .map(|probe| probe.completed.get())
                .sum();
            (host.spawns() - before, completed)
        })
    });
    assert_eq!(
        completed, 100,
        "every request was served on the one connection"
    );
    assert_eq!(spawned, 0, "the driver spawns no task for any request");
}

#[test]
fn keep_alive_serves_three_sequential_requests() {
    let reads = vec![
        ReadAction::Data(b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
        ReadAction::Data(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
        ReadAction::Data(b"GET /2 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec()),
    ];
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"hello")]);
    assert_eq!(owned.error, None);
    let urls: Vec<_> = probes.iter().map(|probe| probe.url.clone()).collect();
    assert_eq!(urls, [b"/0".to_vec(), b"/1".to_vec(), b"/2".to_vec()]);
    assert!(probes.iter().all(|probe| probe.completed == 1));
    assert_eq!(
        owned
            .output
            .windows(b"HTTP/1.1 200".len())
            .filter(|window| *window == b"HTTP/1.1 200")
            .count(),
        3,
        "three responses on the one transport"
    );
    assert!(owned.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(owned.close, CloseKind::ShutdownWrite);
}

#[test]
fn serial_exchanges_do_not_overlap() {
    take_events();
    let reads = vec![ReadAction::Data(
        b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\nGET /1 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"
            .to_vec(),
    )];
    let ((owned, probes), events) = run_on_host(move |context| {
        Box::pin(async move {
            take_events();
            let (transport, state) = ScriptedTransport::new(reads);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let result = session.run().await;
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            ((wire_of(&state, result), observed), take_events())
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 2);
    assert_eq!(
        events,
        [
            ("new", 0),
            ("complete", 0),
            ("drop", 0),
            ("new", 1),
            ("complete", 1),
            ("drop", 1)
        ],
        "the second request's exchange is created only after the first retires"
    );
}

/// A worker whose teardown deadline has passed destroys an owned downstream midway through a
/// request body, before its application and native I/O cleanup joined. The driver still cancels
/// the exchange and resets the transport, and the worker reports `TeardownDeadlineExceeded`
/// instead of a panic.
#[test]
fn a_forced_worker_teardown_abandons_an_owned_downstream_mid_request() {
    let (inspected_tx, inspected_rx) = std::sync::mpsc::channel();
    let completion = forced_teardown_of_scope(move |context| {
        // The second read is never notified and ignores cancellation, so the driver's native I/O
        // cleanup cannot join before the forced teardown destroys it.
        let (transport, state) = ScriptedTransport::new([
            ReadAction::Data(
                b"POST /upload HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nhal".to_vec(),
            ),
            ReadAction::WaitData(Arc::new(tokio::sync::Notify::new()), Vec::new()),
        ]);
        let (application, probes) = H1App::new(vec![Response::fixed(200, b"ok")]);
        let (session, handle) =
            OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
        after_forced_teardown(move || {
            let cancelled: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.cancelled.borrow().clone())
                .collect();
            inspected_tx
                .send((state.borrow().close, cancelled))
                .unwrap();
        });
        Box::pin(async move {
            let _ = session.run().await;
            drop(handle);
        })
    });
    assert_eq!(
        completion,
        Err(ManagedWorkerError::TeardownDeadlineExceeded),
        "the forced destruction of the unjoined driver is a clean overrun"
    );
    let (close, cancelled) = inspected_rx.recv().unwrap();
    assert_eq!(
        cancelled,
        [vec![(HttpErrorCode::CORO_CANCELLED, false)]],
        "the request's exchange was live and is cancelled"
    );
    assert_eq!(
        close,
        CloseKind::Reset,
        "the abandoned driver reset its transport"
    );
}

/// Outside a forced teardown, dropping a started owned downstream before its cleanup joined
/// still asserts.
#[test]
fn dropping_a_started_owned_downstream_before_its_cleanup_joined_asserts() {
    let message = run_on_host(|context| {
        Box::pin(async move {
            let (transport, _state) = ScriptedTransport::new([]);
            let (application, _probes) = H1App::new(vec![]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = Box::pin(session.run());
            assert!(poll_once(run.as_mut()).is_pending());
            panic_text(catch_unwind(AssertUnwindSafe(|| drop(run))).unwrap_err())
        })
    });
    assert_eq!(
        message,
        "started owned H1 runner dropped before application and native I/O cleanup joined"
    );
}

fn patterned(length: usize) -> Vec<u8> {
    (0..length).map(|index| b'a' + (index % 26) as u8).collect()
}

#[test]
fn content_length_request_body_reaches_the_exchange() {
    let body = patterned(16 << 10);
    let mut request = format!(
        "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    let reads = request
        .chunks(4096)
        .map(|chunk| ReadAction::Data(chunk.to_vec()))
        .collect();
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"ok")]);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert!(!probe.request_eom, "the head arrives before its body");
    assert_eq!(probe.request_body, body, "16 KiB arrive intact");
    assert!(probe.request_finished, "the body ends with EOM");
    assert_eq!(probe.completed, 1);
}

/// Serves one byte string, at most each read's `maximum` at a time, then parks until cancelled.
/// Records writes and how the session closed.
struct ByteTransport {
    pending: RefCell<VecDeque<u8>>,
    eof: bool,
    reads: Cell<usize>,
    state: Rc<RefCell<TransportState>>,
}

impl ByteTransport {
    fn new(bytes: Vec<u8>, eof: bool) -> (Rc<Self>, Rc<RefCell<TransportState>>) {
        let (_, state) = ScriptedTransport::new([]);
        (
            Rc::new(Self {
                pending: RefCell::new(bytes.into()),
                eof,
                reads: Cell::new(0),
                state: Rc::clone(&state),
            }),
            state,
        )
    }
}

impl UniplexTransport for ByteTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.reads.set(self.reads.get() + 1);
        let mut pending = self.pending.borrow_mut();
        if !pending.is_empty() || self.eof {
            let take = maximum.min(pending.len());
            let bytes: Vec<u8> = pending.drain(..take).collect();
            let eof = self.eof && pending.is_empty();
            return Box::pin(std::future::ready(Ok(UniplexRead::new(queue(&bytes), eof))));
        }
        let baton = CancellableBaton::with_host(context.host().clone());
        Box::pin(async move {
            let _ = baton.wait(cancellation).await;
            Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"byte transport read cancelled",
            ))
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let length = data.chain_length();
        let bytes = data.coalesce(length).unwrap().to_vec();
        self.state.borrow_mut().writes.push(bytes);
        Box::pin(std::future::ready(Ok(WriteInfo {
            bytes_written: length,
        })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow_mut().close = CloseKind::ShutdownWrite;
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow_mut().close = CloseKind::Closed;
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow_mut().close = CloseKind::Reset;
        Ok(())
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        Err(ByteEventRegistrationError::Unsupported)
    }
}

#[test]
fn request_body_beyond_the_buffer_limit_pauses_ingress() {
    let body = patterned(256 << 10);
    let expected = body.clone();
    let (error, observed) = run_on_host(move |context| {
        Box::pin(async move {
            let mut request = format!(
                "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            request.extend_from_slice(&body);
            let (transport, state) = ByteTransport::new(request, false);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"ok")]);
            application.body_gate = Some(Rc::clone(&gate));
            let mut options = owned_options(context.clone(), transport.clone());
            options.io.buffer_limit = 64 << 10;
            options.io.ingress_buffer_limit = 16 << 10;
            options.io.maximum_read_size = 16 << 10;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let release = async {
                // Lets the session read until ingress stalls behind the unread body.
                for _ in 0..64 {
                    context.host().yield_to_io().await;
                }
                assert!(
                    transport.pending.borrow().len() > 128 << 10,
                    "ingress stopped reading while the body was unread"
                );
                gate.notify_one();
            };
            let (result, ()) = futures::join!(session.run(), release);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            drop(state);
            (result.err().map(|error| error.code()), observed)
        })
    });
    assert_eq!(error, None, "the body never exceeds the request window");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].request_body, expected, "256 KiB arrive intact");
    assert!(observed[0].request_finished);
    assert_eq!(observed[0].completed, 1);
}

#[test]
fn chunked_request_body_reaches_the_exchange() {
    let reads = vec![
        ReadAction::Data(
            b"POST /c HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec(),
        ),
        ReadAction::Data(b"5\r\nhello\r\n".to_vec()),
        ReadAction::Data(b"1\r\n,\r\n6\r\n world\r\n".to_vec()),
        ReadAction::Data(b"0\r\n\r\n".to_vec()),
    ];
    let (owned, probes) = owned_wire(reads, vec![Response::fixed(200, b"ok")]);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let probe = &probes[0];
    assert!(!probe.request_eom);
    assert_eq!(
        probe.request_body, b"hello, world",
        "every chunk arrives in order"
    );
    assert!(
        probe.request_finished,
        "the last chunk ends the body with EOM"
    );
    assert_eq!(probe.completed, 1);
}

#[test]
fn response_without_length_is_chunked_on_http11() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET /stream HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    let responses = vec![Response {
        content_length: false,
        ..Response::fixed(200, b"event: one\n\n")
    }];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.contains("Transfer-Encoding: chunked\r\n"), "{text:?}");
    assert!(
        text.ends_with("\r\n\r\nc\r\nevent: one\n\n\r\n0\r\n\r\n"),
        "{text:?}"
    );
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
}

/// A response body read that suspends is resumed and read again with either framing, as
/// `UniplexSession` reads it. Any resume status continues the read, as in `UniplexSession`.
#[test]
fn a_suspended_response_body_is_read_after_it_resumes_as_uniplex() {
    let reads = || {
        vec![
            ReadAction::Data(b"GET /stream HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ]
    };
    for content_length in [false, true] {
        for status in [ResumeStatus::Signalled, ResumeStatus::Cancelled] {
            let responses = vec![Response {
                content_length,
                suspend: Some(status),
                ..Response::fixed(200, b"event: one\n\n")
            }];
            let (owned, probes) = owned_wire(reads(), responses.clone());
            let uniplex = uniplex_wire(reads(), responses);
            let case = format!("content_length {content_length}, {status:?}");
            assert_eq!(owned.error, None, "{case}");
            assert_eq!(probes[0].completed, 1, "{case}");
            assert!(probes[0].cancelled.is_empty(), "{case}");
            let text = String::from_utf8_lossy(&owned.output);
            assert!(text.contains("event: one\n\n"), "{case}: {text:?}");
            assert_eq!(text, String::from_utf8_lossy(&uniplex.output), "{case}");
            assert_eq!(owned.close, uniplex.close, "{case}");
            assert_eq!(uniplex.error, None, "{case}");
        }
    }
}

/// Serves one `Connection: close` GET whose response body starts with a `ResumeProbe`
/// suspension. Once the driver has polled the resume and flushed the head, the test resumes it
/// with `status`, or with `None` cancels the session from outside the driver. With
/// `cancel_callback`, the resume is ready immediately and that resume callback cancels the
/// session instead. Returns the wire, the exchange's observations and the resume's drops.
fn suspended_response(
    status: Option<ResumeStatus>,
    cancel_callback: Option<&'static str>,
) -> (Wire, Observed, usize) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
            )]);
            let resume = Rc::new(ResumeProbe::default());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.response_resume = Some(Rc::clone(&resume));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            if let Some(callback) = cancel_callback {
                resume.status.set(Some(ResumeStatus::Signalled));
                *resume.cancel_in.borrow_mut() = Some((callback, handle.clone()));
            }
            let release = async {
                if cancel_callback.is_some() {
                    return;
                }
                while resume.polls.get() == 0 {
                    assert!(
                        probes
                            .borrow()
                            .iter()
                            .all(|probe| probe.cancelled.borrow().is_empty()),
                        "a suspended response must not fail before arming its resume"
                    );
                    context.host().yield_to_io().await;
                }
                assert_eq!(resume.drops.get(), 0, "a pending resume is kept");
                assert_eq!(probes.borrow()[0].response_complete.get(), 0);
                assert!(
                    state.borrow().writes.concat().ends_with(b"\r\n\r\n"),
                    "the head is written while the body is suspended"
                );
                match status {
                    Some(status) => resume.release(status),
                    None => handle.cancel(),
                }
            };
            let (result, ()) = futures::join!(session.run(), release);
            let observed = probes.borrow()[0].observed();
            (wire_of(&state, result), observed, resume.drops.get())
        })
    })
}

/// A response body suspended until a wake from outside the driver is read again once it
/// resumes, with any status, as Uniplex reads it: the wire matches an unsuspended response, and
/// the resume is dropped once.
#[test]
fn owned_h1_resumes_a_suspended_body_without_changing_the_wire() {
    let expected = uniplex_wire(
        vec![ReadAction::Data(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
        )],
        vec![Response::fixed(200, b"hello")],
    );
    for status in [
        ResumeStatus::Signalled,
        ResumeStatus::TimedOut,
        ResumeStatus::NotReady,
        ResumeStatus::Cancelled,
    ] {
        let (wire, probe, resume_drops) = suspended_response(Some(status), None);
        assert_eq!(wire.error, None, "{status:?}");
        assert_eq!(wire.output, expected.output, "{status:?}");
        assert_eq!(wire.close, expected.close, "{status:?}");
        assert_eq!(probe.response_complete, 1, "{status:?}");
        assert_eq!(probe.completed, 1, "{status:?}");
        assert_eq!(probe.dropped, 1, "{status:?}");
        assert!(probe.cancelled.is_empty(), "{status:?}");
        assert_eq!(resume_drops, 1, "{status:?}");
    }
}

/// A session cancelled while its response body is suspended resets the connection after the
/// head, cancels the exchange once and drops the pending resume once.
#[test]
fn owned_h1_cancellation_retires_a_suspended_body_once() {
    let (wire, probe, resume_drops) = suspended_response(None, None);
    assert_eq!(wire.error, Some(HttpErrorCode::CORO_CANCELLED));
    assert_eq!(wire.close, CloseKind::Reset);
    assert!(
        wire.output.ends_with(b"\r\n\r\n"),
        "only the head is written"
    );
    assert_eq!(probe.response_complete, 0);
    assert_eq!(probe.completed, 0);
    assert_eq!(probe.dropped, 1);
    assert_eq!(probe.cancelled, [(HttpErrorCode::CORO_CANCELLED, false)]);
    assert_eq!(resume_drops, 1);
}

/// A cancel from the resume future's poll or drop stops the response before its body is sent,
/// as a cancel from any other exchange callback does. The reset comes before any flush, so the
/// wire is empty either way; `response_complete` shows that the body was never serialized.
#[test]
fn owned_h1_resume_callbacks_cannot_send_after_cancelling() {
    for callback in ["resume_poll", "resume_drop"] {
        let (wire, probe, resume_drops) = suspended_response(None, Some(callback));
        assert_eq!(
            wire.error,
            Some(HttpErrorCode::CORO_CANCELLED),
            "{callback}"
        );
        assert_eq!(wire.close, CloseKind::Reset, "{callback}");
        assert!(wire.output.is_empty(), "{callback}");
        assert_eq!(probe.response_complete, 0, "{callback}");
        assert_eq!(probe.completed, 0, "{callback}");
        assert_eq!(probe.dropped, 1, "{callback}");
        assert_eq!(
            probe.cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{callback}"
        );
        assert_eq!(resume_drops, 1, "{callback}");
    }
}

/// Serves one request that selects connection close; no peer EOF follows.
fn assert_closes_after_the_response(request: &'static [u8]) {
    let reads = || vec![ReadAction::Data(request.to_vec())];
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].completed, 1);
    assert!(
        owned.output.ends_with(b"\r\n\r\nhello"),
        "the response is written first"
    );
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "then the transport closes"
    );
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn http10_request_closes_after_the_response() {
    assert_closes_after_the_response(b"GET / HTTP/1.0\r\nHost: a\r\n\r\n");
}

#[test]
fn connection_close_request_closes_after_the_response() {
    assert_closes_after_the_response(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
}

#[test]
fn a_closing_request_admits_no_pipelined_request() {
    for first in [
        &b"GET /0 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"[..],
        &b"GET /0 HTTP/1.0\r\nHost: a\r\n\r\n"[..],
    ] {
        let mut read = first.to_vec();
        read.extend_from_slice(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n");
        let reads = || vec![ReadAction::Data(read.clone())];
        let responses = vec![Response::fixed(200, b"hello")];
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let label = String::from_utf8_lossy(first);
        assert_eq!(owned.error, None, "{label:?}");
        let urls: Vec<_> = probes.iter().map(|probe| probe.url.clone()).collect();
        assert_eq!(
            urls,
            [b"/0".to_vec()],
            "{label:?}: only the closing request is admitted"
        );
        assert_eq!(probes[0].completed, 1, "{label:?}");
        assert_eq!(
            owned
                .output
                .windows(b" 200 ".len())
                .filter(|window| *window == b" 200 ")
                .count(),
            1,
            "{label:?}: one response is written: {:?}",
            String::from_utf8_lossy(&owned.output)
        );
        assert!(owned.output.ends_with(b"\r\n\r\nhello"), "{label:?}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "{label:?}");
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label:?}"
        );
        assert_eq!(owned.close, uniplex.close, "{label:?}");
        assert_eq!(owned.error, uniplex.error, "{label:?}");
    }
}

#[test]
fn peer_eof_mid_request_cancels_the_exchange() {
    let eof: fn() -> ReadAction = || ReadAction::Eof;
    let read_error: fn() -> ReadAction = || ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR);
    for (last_read, cause) in [
        (eof, HttpErrorCode::TRANSPORT_EOF),
        (read_error, HttpErrorCode::TRANSPORT_READ_ERROR),
    ] {
        let reads = || {
            vec![
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
                ),
                last_read(),
            ]
        };
        let (owned, probes) = owned_wire(reads(), vec![Response::fixed(200, b"hello")]);
        let (uniplex, uniplex_requests) =
            uniplex_wire_observed(reads(), vec![Response::fixed(200, b"hello")]);
        assert_eq!(probes.len(), 1);
        let probe = &probes[0];
        assert_eq!(
            probe.cancelled,
            [(cause, true)],
            "peer EOF or a failed read cancels the exchange once, as a peer reset"
        );
        assert_same_requests(
            &probes,
            &uniplex_requests,
            &format!("{cause:?}: the head and body read before the end reach the exchange"),
        );
        assert_eq!(probe.completed, 0);
        assert_eq!(probe.dropped, 1);
        assert_eq!(owned.error, Some(cause));
        assert_eq!(owned.close, CloseKind::Reset);
        assert_eq!(
            without_dates(&owned.output),
            without_dates(&uniplex.output),
            "{cause:?}: the error response, if any, is the one UniplexSession writes"
        );
        assert_eq!(owned.close, uniplex.close, "{cause:?}");
        assert_eq!(owned.error, uniplex.error, "{cause:?}");
    }
}

/// Serves `reads` with the owned driver (`owned`) or `UniplexSession` under the I/O
/// configuration `io`.
fn configured_wire(
    owned: bool,
    reads: Vec<ReadAction>,
    response: Response,
    io: UniplexConfig,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (result, probes) = if owned {
                let (application, probes) = H1App::new(vec![response]);
                let mut options = owned_options(context, transport);
                options.io = io;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                (session.run().await, probes)
            } else {
                let handler = Rc::new(ScriptHandler::new(vec![response]));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    io,
                )
                .unwrap();
                (session.run().await, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// A read error that follows request body bytes the parser has not reached, because the unread
/// body is at `ingress_buffer_limit`, fails the connection only after those bytes reach the
/// exchange, as `UniplexSession` delivers them before it observes the failed read.
#[test]
fn bytes_read_before_a_read_error_reach_the_exchange_as_uniplex() {
    let body = patterned(4096);
    let mut first = b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 4096\r\n\r\n".to_vec();
    first.extend_from_slice(&body[..1536]);
    let reads = || {
        vec![
            ReadAction::Data(first.clone()),
            ReadAction::Data(body[1536..3072].to_vec()),
            ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
        ]
    };
    let io = UniplexConfig {
        ingress_buffer_limit: 1024,
        ..h1_config()
    };
    let response = Response::fixed(200, b"ok");
    let (owned, owned_requests) = configured_wire(true, reads(), response.clone(), io.clone());
    let (uniplex, uniplex_requests) = configured_wire(false, reads(), response, io);
    assert_eq!(owned_requests.len(), 1);
    assert_eq!(uniplex_requests.len(), 1);
    assert_eq!(
        uniplex_requests[0].request_body,
        body[..3072],
        "UniplexSession delivers both reads' body bytes"
    );
    assert_eq!(
        (
            owned_requests[0].method,
            owned_requests[0].request_body.len()
        ),
        (
            uniplex_requests[0].method,
            uniplex_requests[0].request_body.len()
        ),
        "the request head and body bytes read before the error reach the exchange"
    );
    assert_same_requests(&owned_requests, &uniplex_requests, "read error");
    // The owned exchange receives the failure through `cancel`, the handler through its body.
    assert_eq!(
        owned_requests[0].cancelled,
        [(HttpErrorCode::TRANSPORT_READ_ERROR, true)]
    );
    assert_eq!(
        uniplex_requests[0].request_error,
        Some(HttpErrorCode::TRANSPORT_READ_ERROR)
    );
    assert_eq!(owned.error, uniplex.error);
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(without_dates(&owned.output), without_dates(&uniplex.output));
}

/// Serves `reads` with both sessions under `io` and asserts the same requests, output, close
/// and result; returns the owned run and what each application observed.
fn assert_read_error_matches_uniplex(
    reads: impl Fn() -> Vec<ReadAction>,
    response: Response,
    io: UniplexConfig,
    label: &str,
) -> (Wire, Vec<Observed>, Vec<Observed>) {
    let (owned, owned_requests) = configured_wire(true, reads(), response.clone(), io.clone());
    let (uniplex, uniplex_requests) = configured_wire(false, reads(), response, io);
    assert_same_requests(&owned_requests, &uniplex_requests, label);
    assert_eq!(
        without_dates(&owned.output),
        without_dates(&uniplex.output),
        "{label}: the responses written before the failure"
    );
    assert_eq!(owned.close, uniplex.close, "{label}");
    assert_eq!(owned.error, uniplex.error, "{label}");
    (owned, owned_requests, uniplex_requests)
}

/// A read error that follows a pipelined request, read while the request before it awaits its
/// response, fails the connection only after that response. `UniplexSession` does not issue a
/// read while it holds a pipelined request back, so a keep-alive request's successor is parsed
/// and accepted, and a closing request's connection closes without reporting the read error.
#[test]
fn a_pipelined_request_read_before_a_read_error_is_served_as_uniplex() {
    let read_error = Some(HttpErrorCode::TRANSPORT_READ_ERROR);
    for (first, admitted, error) in [
        (&b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\n"[..], 2, read_error),
        (
            &b"GET /0 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"[..],
            1,
            None,
        ),
    ] {
        let mut read = first.to_vec();
        read.extend_from_slice(b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n");
        let reads = || {
            vec![
                ReadAction::Data(read.clone()),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]
        };
        let label = String::from_utf8_lossy(first).into_owned();
        let (owned, owned_requests, uniplex_requests) = assert_read_error_matches_uniplex(
            reads,
            Response::fixed(200, b"hello"),
            h1_config(),
            &label,
        );
        assert_eq!(owned_requests.len(), admitted, "{label}");
        assert_eq!(
            owned_requests[0].completed, 1,
            "{label}: the first request is served"
        );
        assert!(owned_requests[0].cancelled.is_empty(), "{label}");
        assert_eq!(owned.error, error, "{label}");
        if admitted == 2 {
            assert_eq!(
                owned_requests[1].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{label}: the failure cancels the pipelined request before its response"
            );
            assert_eq!(owned_requests[1].headers_polled, 0, "{label}");
            // `UniplexSession` does not give the handler a cause. It drops the pipelined
            // request's response unread, and the read error is the run's error.
            assert!(uniplex_requests[0].response_stopped.is_empty(), "{label}");
            assert_eq!(
                uniplex_requests[1].response_stopped,
                [None],
                "{label}: UniplexSession also ends the pipelined request unserved"
            );
        }
    }
}

/// A read error that follows bytes read behind an upgrade or CONNECT head fails the connection
/// only after the final response resolves the upgrade. `UniplexSession` does not issue a read
/// while the upgrade is pending, so an accepted tunnel's head is written before the reset, and
/// a rejected upgrade closes without reporting the read error.
#[test]
fn bytes_behind_a_pending_upgrade_precede_a_read_error_as_uniplex() {
    let mut refused = WEBSOCKET_HEAD.to_vec();
    refused.extend_from_slice(b"GET /later HTTP/1.1\r\nHost: a\r\n\r\n");
    let refusal = Response {
        tunnel: true,
        ..Response::fixed(400, b"no")
    };
    let read_error = Some(HttpErrorCode::TRANSPORT_READ_ERROR);
    let cases = [
        (
            [CONNECT_HEAD, &[b'x'; 10][..]].concat(),
            Response::tunnel(200, &[b'y'; 10], false),
            "CONNECT accepted",
            read_error,
        ),
        (
            [WEBSOCKET_HEAD, &[b'x'; 10][..]].concat(),
            Response::tunnel(200, &[b'y'; 10], true),
            "WebSocket accepted",
            read_error,
        ),
        (refused, refusal, "WebSocket refused", None),
    ];
    for (read, response, label, error) in cases {
        let reads = || {
            vec![
                ReadAction::Data(read.clone()),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]
        };
        let (owned, owned_requests, _) =
            assert_read_error_matches_uniplex(reads, response, h1_config(), label);
        assert_eq!(owned_requests.len(), 1, "{label}");
        assert_eq!(owned.error, error, "{label}");
        if error.is_some() {
            assert!(
                owned.output.starts_with(b"HTTP/1.1 "),
                "{label}: the accepting head is written before the reset"
            );
            assert_eq!(
                owned_requests[0].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{label}"
            );
        }
    }
}

#[test]
fn drain_closes_after_the_in_flight_response() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            )]);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.body_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let drain = async {
                while probes.borrow().is_empty() {
                    context.host().yield_to_io().await;
                }
                assert!(
                    handle.initiate_drain().unwrap(),
                    "the first drain request changes state"
                );
                assert!(!handle.initiate_drain().unwrap(), "a second one does not");
                gate.notify_one();
            };
            let (result, ()) = futures::join!(session.run(), drain);
            assert!(
                !handle.initiate_drain().unwrap(),
                "a finished session ignores drain"
            );
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        text.contains("\r\nConnection: close\r\n"),
        "the in-flight response carries Connection: close: {text:?}"
    );
    assert!(text.ends_with("\r\n\r\nhello"));
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "then the transport closes"
    );
}

/// The handshake completes when the driver delivers the first request head, after validating
/// it: not before the run, not while the head is still arriving although its exchange exists,
/// and already when the application's `request` runs.
#[test]
fn h1_handshake_complete_after_first_head() {
    let (before, partial_head, at_request, delivered, after, wire) = run_on_host(|context| {
        Box::pin(async move {
            let rest = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n".to_vec()),
                ReadAction::WaitData(Arc::clone(&rest), b"\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, handle) =
                OwnedH1Session::new_with(owned_options(context.clone(), transport), |handle| {
                    let mut application = application;
                    application.session = Some(handle.clone());
                    application
                })
                .unwrap();
            let before = handle.handshake_complete();
            let script = async {
                // The request line opened the exchange, and the driver waits for the rest of
                // the head.
                for _ in 0..64 {
                    if probes.borrow().len() == 1 && state.borrow().reads.len() == 1 {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                let partial_head = (probes.borrow().len() == 1 && state.borrow().reads.len() == 1)
                    .then(|| handle.handshake_complete());
                rest.notify_one();
                for _ in 0..64 {
                    if probes.borrow()[0].method.borrow().is_some() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                let delivered = probes.borrow()[0]
                    .method
                    .borrow()
                    .is_some()
                    .then(|| handle.handshake_complete());
                (partial_head, delivered)
            };
            let (result, (partial_head, delivered)) = futures::join!(session.run(), script);
            let at_request = probes.borrow()[0].handshake_at_request.get();
            (
                before,
                partial_head,
                at_request,
                delivered,
                handle.handshake_complete(),
                wire_of(&state, result),
            )
        })
    });
    assert!(!before, "a session that has not run has no handshake");
    assert_eq!(
        partial_head,
        Some(false),
        "an exchange whose head is still arriving does not complete the handshake"
    );
    assert_eq!(
        at_request,
        Some(true),
        "the handshake is complete when the application's request runs"
    );
    assert_eq!(
        delivered,
        Some(true),
        "a delivered head completed the handshake"
    );
    assert!(after, "the handshake stays complete");
    assert_eq!(wire.error, None);
    assert!(
        wire.output.ends_with(b"\r\n\r\nhello"),
        "{:?}",
        String::from_utf8_lossy(&wire.output)
    );
}

/// The handle reports the session finished only once its run completed: not before the run, and
/// not while the served connection waits for its next request.
#[test]
fn is_finished_after_run_completes() {
    let (before, waiting, after, wire) = run_on_host(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
                ReadAction::WaitEof(Arc::clone(&eof)),
            ]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let before = handle.is_finished();
            let served = || {
                probes
                    .borrow()
                    .first()
                    .is_some_and(|probe| probe.completed.get() == 1)
                    && state.borrow().reads.is_empty()
            };
            let script = async {
                for _ in 0..64 {
                    if served() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                let waiting = served().then(|| handle.is_finished());
                eof.notify_one();
                waiting
            };
            let (result, waiting) = futures::join!(session.run(), script);
            (
                before,
                waiting,
                handle.is_finished(),
                wire_of(&state, result),
            )
        })
    });
    assert!(!before, "a session that has not run has not finished");
    assert_eq!(
        waiting,
        Some(false),
        "a run waiting for the next request has not finished"
    );
    assert!(after, "a completed run has finished");
    assert_eq!(wire.error, None);
}

#[test]
fn turn_budget_yields_to_host_io() {
    for mode in [YieldMode::Wake, YieldMode::Immediate, YieldMode::Deferred] {
        let (yields, completed_yields, completed) = run_on_host_with(move |host, context| {
            Box::pin(async move {
                host.set_yield_mode(mode);
                let (transport, _state) =
                    ScriptedTransport::new([ReadAction::Data(pipelined_gets(100))]);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                session.run().await.unwrap();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (host.yields(), host.completed_yields(), completed)
            })
        });
        assert_eq!(completed, 100);
        assert!(
            yields >= 1,
            "always-ready input longer than one turn budget yields to host I/O"
        );
        assert_eq!(
            completed_yields, yields,
            "the driver resumes only through a completed yield"
        );
    }
}

#[test]
fn cancel_tears_down_cleanly() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            )]);
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.body_gate = Some(Rc::clone(&gate));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let cancel = async {
                while probes.borrow().is_empty() {
                    context.host().yield_to_io().await;
                }
                handle.cancel();
            };
            let (result, ()) = futures::join!(session.run(), cancel);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(
        owned.error,
        Some(HttpErrorCode::CORO_CANCELLED),
        "run completes"
    );
    assert_eq!(probes.len(), 1);
    assert_eq!(
        probes[0].cancelled,
        [(HttpErrorCode::CORO_CANCELLED, false)],
        "the live exchange is cancelled once"
    );
    assert_eq!(probes[0].completed, 0);
    assert_eq!(
        probes[0].dropped, 1,
        "the exchange is dropped before run completes"
    );
    assert_eq!(owned.close, CloseKind::Reset);
    assert!(owned.output.is_empty());
}

#[test]
fn owned_h1_rejects_unsupported_config() {
    type Change = fn(&mut UniplexConfig);
    let cases: [(&str, Change, ByteEventCapability); 10] = [
        (
            "read_timeout",
            |io| io.read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "stream_read_timeout",
            |io| io.stream_read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "write_timeout",
            |io| io.write_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "drain_request_body_on_drop",
            |io| io.drain_request_body_on_drop = false,
            ByteEventCapability::Unsupported,
        ),
        (
            "wait_for_http1_response_on_drain",
            |io| io.wait_for_http1_response_on_drain = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "deliver_ingress_before_abort",
            |io| io.deliver_ingress_before_abort = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "byte-event-capable transport",
            |_| {},
            ByteEventCapability::TransportEvents,
        ),
        (
            "zero maximum_write_size",
            |io| io.maximum_write_size = 0,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size above buffer_limit",
            |io| io.maximum_write_size = io.buffer_limit + 1,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size with no room beside chunk framing",
            |io| io.maximum_write_size = 17,
            ByteEventCapability::Unsupported,
        ),
    ];
    for (name, change, capability) in cases {
        let rejected = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, _state) = ScriptedTransport::with_capability([], capability);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let mut options = owned_options(context, transport);
                change(&mut options.io);
                let rejected = match OwnedH1Session::new(application, options) {
                    Ok(_) => None,
                    Err(error) => Some(error.code()),
                };
                assert!(probes.borrow().is_empty());
                rejected
            })
        });
        assert_eq!(
            rejected,
            Some(HttpErrorCode::INVALID_STATE_TRANSITION),
            "{name} is rejected"
        );
    }
}

#[test]
fn response_head_reports_its_encoded_size() {
    let (owned, probes) = owned_wire_with(
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        vec![Response::fixed(200, b"hello")],
        true,
    );
    assert_eq!(owned.error, None);
    let head = owned
        .output
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head")
        + 4;
    assert_eq!(probes[0].headers_encoded, vec![head as u32]);
}

#[test]
fn response_byte_event_registrations_are_refused() {
    let (owned, probes) = owned_wire_with(
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        vec![Response::fixed(200, b"hello")],
        true,
    );
    assert_eq!(owned.error, None);
    assert!(owned.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(
        probes[0].byte_events,
        vec![Some(HttpErrorCode::INVALID_STATE_TRANSITION); 2],
        "the head's and the body's registrations are each cancelled with the documented error"
    );
}

#[test]
fn request_body_ending_at_the_ingress_limit_completes() {
    let body = patterned(4096);
    let expected = body.clone();
    let observed = run_on_host(move |context| {
        Box::pin(async move {
            let mut request = format!(
                "POST /upload HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            request.extend_from_slice(&body);
            let (transport, _state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"ok")]);
            let mut options = owned_options(context, transport);
            options.io.ingress_buffer_limit = body.len() as u32;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let result = session.run().await;
            assert_eq!(result.err().map(|error| error.code()), None);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            observed
        })
    });
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].request_body, expected);
    assert!(
        observed[0].request_finished,
        "the last body bytes reach the ingress limit, and the request still ends with EOM"
    );
    assert_eq!(observed[0].completed, 1);
}

/// The `record_buffered_read_bytes` deltas a session reported.
#[derive(Default)]
pub(super) struct BufferedReadDeltas(pub(super) RefCell<Vec<i64>>);

impl crate::http2::stats::Http2SessionStats for BufferedReadDeltas {
    fn record_buffered_read_bytes(&self, delta: i64) {
        self.0.borrow_mut().push(delta);
    }
}

/// Takes this thread's count of `bytes_processed` callbacks that owned drivers, or Uniplex
/// sessions, received.
pub(super) fn take_ingress_credit_callbacks(owned: bool) -> usize {
    if owned {
        crate::owned_h1::take_ingress_credit_callbacks_for_test()
    } else {
        crate::uniplex::take_ingress_credit_callbacks_for_test()
    }
}

/// What one server observed of [`resume_threshold_run`]'s request.
struct ResumeThresholdRun {
    wire: Wire,
    observed: Vec<Observed>,
    /// `bytes_processed` callbacks the request source gave its session.
    callbacks: usize,
    /// The `record_buffered_read_bytes` deltas of a `UniplexSession` that reports stats.
    buffered_read_deltas: Vec<i64>,
}

/// A 22 KiB request body, between `ingress_buffer_limit` (16 KiB) and `buffer_limit` (64 KiB),
/// read 4 KiB at a time. The first transport read carries 20 KiB of it, which pause parsing;
/// the last 2 KiB wait behind that pause, and with them the request's end. Every read arrives
/// as one buffer, so parsing pauses once. With `stats`, a `UniplexSession` reports session stats.
fn resume_threshold_run(owned: bool, stats: bool) -> ResumeThresholdRun {
    let body = patterned(22 << 10);
    let mut first = format!(
        "POST /u HTTP/1.1\r\nHost: a\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    first.extend_from_slice(&body[..20 << 10]);
    let reads = vec![
        ReadAction::Data(first),
        ReadAction::Data(body[20 << 10..].to_vec()),
    ];
    let io = UniplexConfig {
        buffer_limit: 64 << 10,
        ingress_buffer_limit: 16 << 10,
        maximum_read_size: 64 << 10,
        read_allocation_size: 64 << 10,
        ..h1_config()
    };
    let response = Response::fixed(200, b"ok");
    run_on_host(move |context| {
        Box::pin(async move {
            take_ingress_credit_callbacks(owned);
            let deltas = Rc::new(BufferedReadDeltas::default());
            let (transport, state) = ScriptedTransport::new(reads);
            let (result, probes) = if owned {
                let (mut application, probes) = H1App::new(vec![response]);
                application.body_read_size = 4 << 10;
                let mut options = owned_options(context, transport);
                options.io = io;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                (session.run().await, probes)
            } else {
                let mut handler = ScriptHandler::new(vec![response]);
                handler.body_read_size = 4 << 10;
                let handler = Rc::new(handler);
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = if stats {
                    UniplexSession::new_with_stats(
                        context,
                        TransportDirection::Downstream,
                        transport,
                        Some(erased),
                        io,
                        Rc::clone(&deltas) as Rc<dyn crate::http2::stats::Http2SessionStats>,
                    )
                } else {
                    UniplexSession::new(
                        context,
                        TransportDirection::Downstream,
                        transport,
                        Some(erased),
                        io,
                    )
                }
                .unwrap();
                (session.run().await, Rc::clone(&handler.probes))
            };
            let callbacks = take_ingress_credit_callbacks(owned);
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            ResumeThresholdRun {
                wire: wire_of(&state, result),
                observed,
                callbacks,
                buffered_read_deltas: deltas.0.take(),
            }
        })
    })
}

/// Asserts that `run` received the whole body and responded to it, after one pause and the one
/// credit callback that resumed parsing.
fn assert_resumed_once_below_the_threshold(run: &ResumeThresholdRun) {
    assert_eq!(run.wire.error, None);
    assert!(
        run.wire.output.starts_with(b"HTTP/1.1 200 ") && run.wire.output.ends_with(b"\r\n\r\nok"),
        "{:?}",
        String::from_utf8_lossy(&run.wire.output)
    );
    assert_eq!(run.observed.len(), 1);
    assert!(
        run.observed[0].request_body == patterned(22 << 10),
        "the body arrives intact: {} bytes",
        run.observed[0].request_body.len()
    );
    assert!(
        run.observed[0].request_finished,
        "parsing resumed and reached the request's end"
    );
    assert_eq!(
        run.callbacks, 1,
        "only the read that takes the unread body below the threshold reports credit"
    );
}

#[test]
fn uniplex_server_resumes_below_the_ingress_threshold() {
    assert_resumed_once_below_the_threshold(&resume_threshold_run(false, false));
}

#[test]
fn owned_server_resumes_below_the_ingress_threshold() {
    assert_resumed_once_below_the_threshold(&resume_threshold_run(true, false));
}

/// HTTP/1.1 session stats count buffered request bytes down on every read, so a Uniplex
/// session that reports them keeps a callback per read.
#[test]
fn uniplex_server_with_stats_keeps_a_callback_per_read() {
    let run = resume_threshold_run(false, true);
    assert_eq!(run.wire.error, None);
    assert!(run.observed[0].request_body == patterned(22 << 10));
    let deltas = &run.buffered_read_deltas;
    let reads: Vec<i64> = deltas.iter().copied().filter(|delta| *delta < 0).collect();
    assert_eq!(
        reads,
        [-4096, -4096, -4096, -4096, -4096, -2048],
        "one delta per 4 KiB read of the 22 KiB body; all deltas: {deltas:?}"
    );
    assert_eq!(
        deltas.iter().sum::<i64>(),
        0,
        "every buffered byte is counted out"
    );
    assert_eq!(run.callbacks, 6);
}

#[test]
fn ready_reads_count_against_the_turn_budget() {
    let request = pipelined_gets(5);
    let cases: [(&str, Vec<ReadAction>); 2] = [
        (
            "empty reads",
            (0..200)
                .map(|_| ReadAction::Data(Vec::new()))
                .chain([ReadAction::Data(request.clone())])
                .collect(),
        ),
        (
            "one-byte reads",
            request
                .iter()
                .map(|byte| ReadAction::Data(vec![*byte]))
                .collect(),
        ),
    ];
    for (name, reads) in cases {
        let (yields, completed) = run_on_host_with(move |host, context| {
            Box::pin(async move {
                let (transport, _state) = ScriptedTransport::new(reads);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                session.run().await.unwrap();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (host.yields(), completed)
            })
        });
        assert_eq!(completed, 5, "{name}: every request is served");
        assert!(
            yields >= 1,
            "{name}: more ready reads than one turn budget yield to host I/O"
        );
    }
}

#[test]
fn cancel_from_an_exchange_callback_stops_the_response() {
    for (callback, response) in [
        ("request", Response::fixed(204, b"")),
        ("poll_headers", Response::fixed(204, b"")),
        ("poll_body", Response::fixed(200, b"hello")),
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![response]);
                let slot = Rc::clone(&application.cancel_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::CORO_CANCELLED),
            "{callback}"
        );
        assert_eq!(probes.len(), 1);
        assert_eq!(
            probes[0].response_complete, 0,
            "{callback}: a response polled after the exchange cancelled the session is not serialized"
        );
        if callback == "request" {
            assert_eq!(
                probes[0].headers_polled, 0,
                "an exchange that cancelled the session is not polled again"
            );
        }
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{callback}"
        );
        assert_eq!(probes[0].completed, 0);
        assert!(owned.output.is_empty(), "{callback}");
        assert_eq!(owned.close, CloseKind::Reset);
    }
}

#[test]
fn response_body_polls_fit_the_egress_buffer() {
    let body = patterned(16 << 10);
    for content_length in [true, false] {
        let response = Response {
            content_length,
            ..Response::fixed(200, &body)
        };
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![response]);
                let mut options = owned_options(context, transport);
                options.io.buffer_limit = 16 << 10;
                options.io.maximum_write_size = 16 << 10;
                options.io.ingress_buffer_limit = 4 << 10;
                options.io.maximum_read_size = 4 << 10;
                options.io.read_allocation_size = 4 << 10;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                let result = session.run().await;
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error, None,
            "content_length {content_length}: a body as large as the write size is served"
        );
        assert_eq!(probes[0].completed, 1);
        let head = owned
            .output
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("a response head")
            + 4;
        let wire_body = &owned.output[head..];
        if content_length {
            assert_eq!(wire_body, body.as_slice());
        } else {
            assert!(wire_body.ends_with(b"0\r\n\r\n"));
            assert!(wire_body.len() > body.len());
        }
        assert_eq!(owned.close, CloseKind::ShutdownWrite);
    }
}

#[test]
fn drain_closes_an_idle_keep_alive_connection() {
    let (owned, probes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
            )]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let drain = async {
                while probes
                    .borrow()
                    .first()
                    .is_none_or(|probe| probe.completed.get() == 0)
                {
                    context.host().yield_to_io().await;
                }
                assert!(handle.initiate_drain().unwrap());
            };
            let (result, ()) = futures::join!(session.run(), drain);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    });
    assert_eq!(owned.error, None);
    assert_eq!(probes.len(), 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        !text.contains("Connection: close"),
        "the response was written before the drain: {text:?}"
    );
    assert!(text.ends_with("\r\n\r\nhello"));
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "the idle keep-alive connection closes on drain"
    );
}

#[test]
fn drain_before_a_request_admits_no_exchange() {
    let reads = || {
        vec![ReadAction::Data(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, probes) = run_on_host({
        let reads = reads();
        let responses = responses.clone();
        move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new(reads);
                let (application, probes) = H1App::new(responses);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                assert!(handle.initiate_drain().unwrap());
                let result = session.run().await;
                let observed = probes.borrow().len();
                (wire_of(&state, result), observed)
            })
        }
    });
    let (uniplex, served) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads());
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            assert!(handle.initiate_drain().unwrap());
            let result = session.run().await;
            (wire_of(&state, result), handler.served.get())
        })
    });
    assert_eq!(uniplex.error, None);
    assert_eq!(
        served, 0,
        "UniplexSession admits no request after an idle drain"
    );
    assert_eq!(probes, 0, "nor does the owned driver");
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.error, None);
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn request_chunked_trailers_reach_the_exchange() {
    // A body then trailers, and trailers right after the head.
    for (chunks, body) in [
        (
            &b"5\r\nhello\r\n0\r\nx-checksum: 1\r\n\r\n"[..],
            &b"hello"[..],
        ),
        (&b"0\r\nx-checksum: 1\r\n\r\n"[..], &b""[..]),
    ] {
        let mut request =
            b"POST /c HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec();
        request.extend_from_slice(chunks);
        let reads = || vec![ReadAction::Data(request.clone())];
        let label = String::from_utf8_lossy(chunks);
        let responses = vec![Response::fixed(200, b"ok")];
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(owned.error, None, "{label:?}");
        assert_eq!(probes.len(), 1, "{label:?}");
        let probe = &probes[0];
        assert!(
            !probe.request_eom,
            "{label:?}: the trailers follow the head"
        );
        assert_eq!(probe.request_body, body, "{label:?}");
        assert_eq!(
            probe.request_trailers,
            [(vec![(b"x-checksum".to_vec(), b"1".to_vec())], true)],
            "{label:?}: the trailer section arrives as one trailers event carrying EOM"
        );
        assert!(probe.request_finished, "{label:?}");
        assert_eq!(probe.completed, 1, "{label:?}");
        assert!(probe.cancelled.is_empty(), "{label:?}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label:?}"
        );
        assert_eq!(owned.close, uniplex.close, "{label:?}");
    }
}

#[test]
fn owned_h1_rejects_a_cancelled_context() {
    let rejected = run_on_host(|context| {
        Box::pin(async move {
            context.handle().begin_shutdown();
            assert!(context.is_cancelled());
            let (transport, _state) = ScriptedTransport::new([]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let rejected = match OwnedH1Session::new(application, owned_options(context, transport))
            {
                Ok(_) => None,
                Err(error) => Some(error.code()),
            };
            assert!(probes.borrow().is_empty());
            rejected
        })
    });
    assert_eq!(rejected, Some(HttpErrorCode::INVALID_STATE_TRANSITION));
}

/// The first exchange panics in `callback` at retirement. `run` resumes the panic after the
/// connection fails, and later requests are not parsed.
fn assert_retirement_panic_fails_the_connection(callback: &'static str, request: Vec<u8>) {
    let (panic, output, close, probes) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.panic_in = Some(callback);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let panic = futures::FutureExt::catch_unwind(AssertUnwindSafe(session.run()))
                .await
                .err()
                .and_then(|payload| payload.downcast_ref::<&str>().map(|text| text.to_string()));
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            let state = state.borrow();
            (panic, state.writes.concat(), state.close, observed)
        })
    });
    assert_eq!(
        panic.as_deref(),
        Some("exchange panicked at retirement"),
        "{callback}: run resumes the panic"
    );
    assert_eq!(
        probes.len(),
        1,
        "{callback}: no request is parsed after a retirement panic"
    );
    assert!(
        output.ends_with(b"\r\n\r\nhello"),
        "{callback}: the response was written before retirement"
    );
    assert_eq!(close, CloseKind::Reset, "{callback}: the connection fails");
}

#[test]
fn a_panic_in_complete_fails_the_connection() {
    // A second request is already read when the first exchange's `complete` panics.
    assert_retirement_panic_fails_the_connection("complete", pipelined_gets(2));
}

#[test]
fn a_panic_dropping_the_exchange_fails_the_idle_connection() {
    // A keep-alive connection whose input has all been read.
    assert_retirement_panic_fails_the_connection(
        "drop",
        b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
    );
}

/// An exchange's panic that reaches the per-turn catch is reported once, at driver scope. The
/// run still resumes it.
#[test]
fn owned_h1_driver_turn_panic_is_reported_once() {
    let (panic, reports, counts) = run_on_host(|context| {
        Box::pin(async move {
            // Discards reports left on this thread by an earlier test.
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let (transport, _state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
            )]);
            let (mut application, _probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.panic_in = Some("poll_headers");
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let panic = futures::FutureExt::catch_unwind(AssertUnwindSafe(session.run()))
                .await
                .err()
                .map(panic_text);
            let delta = worker.poll_stats().since(&start);
            (
                panic,
                take_contained_panics(),
                (
                    delta.contained_exchange_panics,
                    delta.contained_connection_panics,
                    delta.contained_driver_panics,
                ),
            )
        })
    });
    assert_eq!(panic.as_deref(), Some("exchange panicked polling headers"));
    assert_eq!(
        reports,
        [(
            PanicSite::DriverTurn,
            None,
            Some("exchange panicked polling headers".to_owned())
        )]
    );
    assert_eq!(counts, (0, 0, 1));
}

/// Each byte of `bytes` in its own ready read.
fn one_byte_reads(bytes: &[u8]) -> Vec<ReadAction> {
    bytes
        .iter()
        .map(|byte| ReadAction::Data(vec![*byte]))
        .collect()
}

#[test]
fn cancel_from_a_driver_callback_fails_the_connection() {
    // One-byte reads use up the turn's budget before the first request is parsed, so its
    // response would be written in the same turn.
    for callback in [
        "new_exchange",
        "byte_event",
        "headers_encoded",
        "response_complete",
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new(one_byte_reads(&pipelined_gets(3)));
                let (mut application, probes) = H1App::new(vec![Response::fixed(204, b"")]);
                application.instrument = matches!(callback, "byte_event" | "headers_encoded");
                let slot = Rc::clone(&application.cancel_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::CORO_CANCELLED),
            "{callback}"
        );
        assert_eq!(probes.len(), 1, "{callback}");
        if callback == "new_exchange" {
            assert!(
                probes[0].method.is_none(),
                "an exchange that cancelled the session from its creation gets no request"
            );
        }
        assert_eq!(
            probes[0].response_complete,
            usize::from(callback == "response_complete"),
            "{callback}: the response is not serialized after the cancel"
        );
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{callback}"
        );
        assert_eq!(probes[0].completed, 0, "{callback}");
        assert!(
            owned.output.is_empty(),
            "{callback}: nothing is written after the cancel"
        );
        assert_eq!(owned.close, CloseKind::Reset, "{callback}");
    }
}

#[test]
fn drain_from_an_exchange_callback_closes_after_that_response() {
    for callback in ["request", "poll_headers"] {
        let (owned, completed) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                )]);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let slot = Rc::clone(&application.drain_in);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some((callback, handle));
                let result = session.run().await;
                slot.borrow_mut().take();
                let completed: usize = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.completed.get())
                    .sum();
                (wire_of(&state, result), completed)
            })
        });
        assert_eq!(owned.error, None, "{callback}");
        assert_eq!(completed, 1, "{callback}");
        let text = String::from_utf8_lossy(&owned.output);
        assert!(
            text.contains("\r\nConnection: close\r\n"),
            "{callback}: a drain before the response head makes it carry Connection: close: {text:?}"
        );
        assert!(text.ends_with("\r\n\r\nhello"), "{callback}");
        assert_eq!(
            owned.close,
            CloseKind::ShutdownWrite,
            "{callback}: then the transport closes"
        );
    }
}

/// How one fenced write completes.
#[derive(Clone, Copy, Debug)]
enum WriteOutcome {
    Full,
    /// Success that reports one byte fewer than requested.
    Short,
    /// Failure with `WRITE_TIMEOUT` and no bytes written.
    Fail,
}

impl WriteOutcome {
    fn result(self, requested: usize) -> Result<WriteInfo, TransportWriteError> {
        match self {
            Self::Full => Ok(WriteInfo {
                bytes_written: requested,
            }),
            Self::Short => Ok(WriteInfo {
                bytes_written: requested - 1,
            }),
            Self::Fail => Err(TransportWriteError::new(
                HttpError::with_message(HttpErrorCode::WRITE_TIMEOUT, b"fenced write failure"),
                0,
            )),
        }
    }
}

/// Holds every write's completion until `open`. Each write takes the next scripted outcome
/// (then `Full`).
#[derive(Default)]
struct WriteFence {
    open: Cell<bool>,
    outcomes: RefCell<VecDeque<WriteOutcome>>,
    wakers: RefCell<Vec<Waker>>,
    /// Queued writes not yet completed: requested bytes and outcome, in write order.
    receipts: RefCell<VecDeque<(usize, WriteOutcome)>>,
    direct_writes: Cell<usize>,
    queued_writes: Cell<usize>,
    /// When set, the write queue's `is_saturated` returns `false`. Otherwise it returns `true`,
    /// as `UniplexWriteQueue`'s default does.
    unsaturated: Cell<bool>,
    saturation_queries: Cell<usize>,
    /// The write queue's `can_write` calls.
    capacity_checks: Cell<usize>,
    /// Run once, from inside the transport, by the named callback: `read` (a read completes),
    /// `write_complete` (a write completes) or `can_write` (the write queue's capacity check).
    reentry: RefCell<Option<(&'static str, Box<dyn Fn()>)>>,
}

impl WriteFence {
    /// Runs the reentry hook if `callback` is the one it names.
    fn reenter(&self, callback: &str) {
        let hook = self
            .reentry
            .borrow_mut()
            .take_if(|(name, _)| *name == callback);
        if let Some((_, hook)) = hook {
            hook();
        }
    }

    fn open(&self) {
        self.open.set(true);
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    fn poll_open(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.open.get() {
            return Poll::Ready(());
        }
        self.wakers.borrow_mut().push(context.waker().clone());
        Poll::Pending
    }

    fn next_outcome(&self) -> WriteOutcome {
        self.outcomes
            .borrow_mut()
            .pop_front()
            .unwrap_or(WriteOutcome::Full)
    }
}

fn record_write(state: &RefCell<TransportState>, data: BufQueue) -> usize {
    let length = data.chain_length();
    let bytes = data.coalesce(length).unwrap().to_vec();
    state.borrow_mut().writes.push(bytes);
    length
}

/// A write queue whose writes complete, in order, once the fence opens.
struct FenceQueue {
    fence: Rc<WriteFence>,
    state: Rc<RefCell<TransportState>>,
}

impl UniplexWriteQueue for FenceQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        self.fence
            .capacity_checks
            .set(self.fence.capacity_checks.get() + 1);
        self.fence.reenter("can_write");
        self.fence.receipts.borrow().len() < 4
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let length = record_write(&self.state, data);
        self.fence
            .queued_writes
            .set(self.fence.queued_writes.get() + 1);
        let outcome = self.fence.next_outcome();
        self.fence
            .receipts
            .borrow_mut()
            .push_back((length, outcome));
        Ok(())
    }

    fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<UniplexWriteCompletion> {
        if self.fence.poll_open(context).is_pending() {
            return Poll::Pending;
        }
        let Some((requested_bytes, outcome)) = self.fence.receipts.borrow_mut().pop_front() else {
            self.fence.wakers.borrow_mut().push(context.waker().clone());
            return Poll::Pending;
        };
        self.fence.reenter("write_complete");
        Poll::Ready(UniplexWriteCompletion {
            requested_bytes,
            result: outcome.result(requested_bytes),
        })
    }

    fn is_idle(&self) -> bool {
        self.fence.receipts.borrow().is_empty()
    }

    fn is_saturated(&self) -> bool {
        self.fence
            .saturation_queries
            .set(self.fence.saturation_queries.get() + 1);
        !self.fence.unsaturated.get()
    }
}

/// Reads through a [`ScriptedTransport`]; writes complete through the fence, directly or, when
/// `queued`, through a [`FenceQueue`].
struct FencedTransport {
    inner: Rc<ScriptedTransport>,
    state: Rc<RefCell<TransportState>>,
    fence: Rc<WriteFence>,
    queued: bool,
}

impl FencedTransport {
    fn new(
        reads: impl IntoIterator<Item = ReadAction>,
        queued: bool,
    ) -> (Rc<Self>, Rc<RefCell<TransportState>>, Rc<WriteFence>) {
        let (inner, state) = ScriptedTransport::new(reads);
        let fence = Rc::new(WriteFence::default());
        (
            Rc::new(Self {
                inner,
                state: Rc::clone(&state),
                fence: Rc::clone(&fence),
                queued,
            }),
            state,
            fence,
        )
    }
}

impl UniplexTransport for FencedTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexWriteQueue>>, HttpError> {
        Ok(self.queued.then(|| {
            Box::new(FenceQueue {
                fence: Rc::clone(&self.fence),
                state: Rc::clone(&self.state),
            }) as Box<dyn UniplexWriteQueue>
        }))
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let read = self
            .inner
            .read(context, maximum, allocation_size, timeout, cancellation);
        let fence = Rc::clone(&self.fence);
        Box::pin(async move {
            let result = read.await;
            fence.reenter("read");
            result
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let length = record_write(&self.state, data);
        self.fence
            .direct_writes
            .set(self.fence.direct_writes.get() + 1);
        let outcome = self.fence.next_outcome();
        let fence = Rc::clone(&self.fence);
        Box::pin(poll_fn(move |context| {
            fence.poll_open(context).map(|()| {
                fence.reenter("write_complete");
                outcome.result(length)
            })
        }))
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

const CLOSING_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
const KEEP_ALIVE_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";

#[test]
fn response_retires_only_after_its_write_completes() {
    for queued in [false, true] {
        let (owned, held, completed, direct_writes, queued_writes) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state, fence) =
                    FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], queued);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let release = async {
                    for _ in 0..64 {
                        if !state.borrow().writes.is_empty() {
                            break;
                        }
                        context.host().yield_to_io().await;
                    }
                    // Yields for the driver turns in which it could wrongly retire the exchange.
                    for _ in 0..8 {
                        context.host().yield_to_io().await;
                    }
                    let probe = Rc::clone(&probes.borrow()[0]);
                    let held = (probe.response_complete.get(), probe.completed.get());
                    fence.open();
                    held
                };
                let (result, held) = futures::join!(session.run(), release);
                let completed = probes.borrow()[0].completed.get();
                (
                    wire_of(&state, result),
                    held,
                    completed,
                    fence.direct_writes.get(),
                    fence.queued_writes.get(),
                )
            })
        });
        assert_eq!(
            held,
            (1, 0),
            "queued {queued}: a complete response retires only once its write completes"
        );
        assert_eq!(owned.error, None, "queued {queued}");
        assert_eq!(completed, 1, "queued {queued}");
        assert!(owned.output.ends_with(b"\r\n\r\nhello"), "queued {queued}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "queued {queued}");
        if queued {
            assert_eq!(direct_writes, 0, "a write queue carries every write");
            assert!(queued_writes >= 1);
        } else {
            assert_eq!(queued_writes, 0);
            assert!(direct_writes >= 1);
        }
    }
}

#[test]
fn a_failed_or_short_write_fails_the_connection() {
    for queued in [false, true] {
        for (outcome, code) in [
            (WriteOutcome::Short, HttpErrorCode::TRANSPORT_WRITE_ERROR),
            (WriteOutcome::Fail, HttpErrorCode::WRITE_TIMEOUT),
        ] {
            let (owned, probes) = run_on_host(move |context| {
                Box::pin(async move {
                    let (transport, state, fence) =
                        FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], queued);
                    fence.open.set(true);
                    fence.outcomes.borrow_mut().push_back(outcome);
                    let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                    let (session, _handle) =
                        OwnedH1Session::new(application, owned_options(context, transport))
                            .unwrap();
                    let result = session.run().await;
                    let observed: Vec<_> = probes
                        .borrow()
                        .iter()
                        .map(|probe| probe.observed())
                        .collect();
                    (wire_of(&state, result), observed)
                })
            });
            let case = format!("queued {queued}, {outcome:?}");
            assert_eq!(owned.error, Some(code), "{case}");
            assert_eq!(owned.close, CloseKind::Reset, "{case}");
            assert_eq!(probes.len(), 1, "{case}");
            assert_eq!(
                probes[0].cancelled,
                [(code, false)],
                "{case}: the exchange whose write failed is cancelled, not retired"
            );
            assert_eq!(probes[0].completed, 0, "{case}");
            assert_eq!(probes[0].dropped, 1, "{case}");
        }
    }
}

/// Serves a keep-alive GET with the owned driver (`owned`) or `UniplexSession` and fails the
/// next read while the write of `response` is held at the fence; the write then completes with
/// `outcome`. Also returns how the transport was closed before that write completed.
fn read_error_during_write_wire(
    owned: bool,
    queued: bool,
    outcome: WriteOutcome,
    response: Response,
) -> (Wire, Vec<Observed>, CloseKind) {
    run_on_host(move |context| {
        Box::pin(async move {
            let failed = Arc::new(tokio::sync::Notify::new());
            let (transport, state, fence) = FencedTransport::new(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
                    ReadAction::WaitError(Arc::clone(&failed), HttpErrorCode::TRANSPORT_READ_ERROR),
                ],
                queued,
            );
            fence.outcomes.borrow_mut().push_back(outcome);
            let script = async {
                for _ in 0..64 {
                    if !state.borrow().writes.is_empty() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                assert!(
                    !state.borrow().writes.is_empty(),
                    "the response write starts"
                );
                failed.notify_one();
                // Yields for the session turns in which it observes the failed read.
                for _ in 0..8 {
                    context.host().yield_to_io().await;
                }
                let close = state.borrow().close;
                fence.open();
                close
            };
            let (result, close, probes) = if owned {
                let (application, probes) = H1App::new(vec![response]);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let (result, close) = futures::join!(session.run(), script);
                (result, close, probes)
            } else {
                let handler = Rc::new(ScriptHandler::new(vec![response]));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let (result, close) = futures::join!(session.run(), script);
                (result, close, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed, close)
        })
    })
}

/// A read error that arrives while a response's write is outstanding fails the connection as
/// `UniplexSession` does: the transport resets only once the write completes, and the read
/// error stays the run's error even when that write then fails too.
#[test]
fn a_read_error_during_a_response_write_fails_the_connection_as_uniplex() {
    for queued in [false, true] {
        for outcome in [WriteOutcome::Full, WriteOutcome::Fail] {
            let case = format!("queued {queued}, {outcome:?}");
            let response = Response::fixed(204, b"");
            let (owned, owned_requests, owned_held) =
                read_error_during_write_wire(true, queued, outcome, response.clone());
            let (uniplex, uniplex_requests, uniplex_held) =
                read_error_during_write_wire(false, queued, outcome, response);
            assert_same_requests(&owned_requests, &uniplex_requests, &case);
            assert_eq!(
                (owned_held, uniplex_held),
                (CloseKind::Open, CloseKind::Open),
                "{case}: neither session resets while the write is outstanding"
            );
            assert_eq!(
                without_dates(&owned.output),
                without_dates(&uniplex.output),
                "{case}: the response written before the reset"
            );
            assert_eq!(owned.close, uniplex.close, "{case}");
            assert_eq!(owned.error, uniplex.error, "{case}: the run's error");
            assert_eq!(
                owned.error,
                Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                "{case}"
            );
            assert_eq!(owned.close, CloseKind::Reset, "{case}");
            assert_eq!(
                owned_requests[0].cancelled,
                [(HttpErrorCode::TRANSPORT_READ_ERROR, true)],
                "{case}: the failed read cancels the exchange as the peer's failure"
            );
        }
    }
}

/// As `a_read_error_during_a_response_write_fails_the_connection_as_uniplex`, with a response
/// body. Both sessions write the ready body serialized with the head before the read fails.
#[test]
fn a_read_error_during_a_response_write_writes_the_serialized_body() {
    for queued in [false, true] {
        let case = format!("queued {queued}");
        let response = Response::fixed(200, b"hello");
        let (owned, owned_requests, _) =
            read_error_during_write_wire(true, queued, WriteOutcome::Full, response.clone());
        let (uniplex, uniplex_requests, _) =
            read_error_during_write_wire(false, queued, WriteOutcome::Full, response);
        assert_same_requests(&owned_requests, &uniplex_requests, &case);
        assert_eq!(owned.close, uniplex.close, "{case}");
        assert_eq!(owned.error, uniplex.error, "{case}");
        assert_eq!(
            owned.error,
            Some(HttpErrorCode::TRANSPORT_READ_ERROR),
            "{case}"
        );
        let output = without_dates(&uniplex.output);
        assert!(
            output.ends_with("\r\nContent-Length: 5\r\n\r\nhello"),
            "{case}: the serialized head and ready body are written: {output:?}"
        );
        assert_eq!(without_dates(&owned.output), output, "{case}");
    }
}

#[test]
fn cancel_from_a_transport_callback_fails_the_connection() {
    // A keep-alive GET: only the cancel ends the connection. For `can_write`, one-byte reads
    // use up the turn's budget before the first request is parsed, so its response is written
    // in the turn whose capacity check cancels.
    for (callback, queued) in [
        ("read", false),
        ("write_complete", false),
        ("write_complete", true),
        ("can_write", true),
    ] {
        let (owned, probes) = run_on_host(move |context| {
            Box::pin(async move {
                let (reads, response) = if callback == "can_write" {
                    (
                        one_byte_reads(&pipelined_gets(3)),
                        Response::fixed(204, b""),
                    )
                } else {
                    (
                        vec![ReadAction::Data(
                            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                        )],
                        Response::fixed(200, b"hello"),
                    )
                };
                let (transport, state, fence) = FencedTransport::new(reads, queued);
                fence.open.set(true);
                let (application, probes) = H1App::new(vec![response]);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((callback, Box::new(move || handle.cancel())));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            })
        });
        let case = format!("{callback}, queued {queued}");
        assert_eq!(owned.error, Some(HttpErrorCode::CORO_CANCELLED), "{case}");
        assert_eq!(owned.close, CloseKind::Reset, "{case}");
        if callback == "read" {
            assert!(
                probes.is_empty(),
                "{case}: no exchange is created after the read that cancelled the session"
            );
            assert!(owned.output.is_empty(), "{case}: nothing is written");
            continue;
        }
        assert_eq!(probes.len(), 1, "{case}");
        assert_eq!(
            probes[0].cancelled,
            [(HttpErrorCode::CORO_CANCELLED, false)],
            "{case}: the live exchange is cancelled, not retired"
        );
        assert_eq!(probes[0].completed, 0, "{case}");
        assert_eq!(probes[0].dropped, 1, "{case}");
        if callback == "write_complete" {
            assert!(
                owned.output.ends_with(b"\r\n\r\nhello"),
                "{case}: the response was written before the cancel"
            );
        } else {
            assert!(
                owned.output.is_empty(),
                "{case}: nothing is written after the cancel"
            );
        }
    }
}

#[test]
fn drain_from_a_read_admits_no_exchange() {
    // The read that carries a keep-alive GET drains the idle session from inside the transport.
    let request = || {
        vec![ReadAction::Data(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response::fixed(200, b"hello")];
    let (owned, admitted) = run_on_host({
        let responses = responses.clone();
        move |context| {
            Box::pin(async move {
                let (transport, state, fence) = FencedTransport::new(request(), false);
                fence.open.set(true);
                let (application, probes) = H1App::new(responses);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((
                    "read",
                    Box::new(move || assert!(handle.initiate_drain().unwrap())),
                ));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let admitted = probes.borrow().len();
                (wire_of(&state, result), admitted)
            })
        }
    });
    let (uniplex, served) = run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state, fence) = FencedTransport::new(request(), false);
            fence.open.set(true);
            let handler = Rc::new(ScriptHandler::new(responses));
            let erased: HttpHandlerRef = handler.clone();
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(erased),
                h1_config(),
            )
            .unwrap();
            *fence.reentry.borrow_mut() = Some((
                "read",
                Box::new(move || assert!(handle.initiate_drain().unwrap())),
            ));
            let result = session.run().await;
            fence.reentry.borrow_mut().take();
            (wire_of(&state, result), handler.served.get())
        })
    });
    assert_eq!(uniplex.error, None);
    assert_eq!(
        served, 0,
        "UniplexSession admits no request read with the drain"
    );
    assert_eq!(admitted, 0, "nor does the owned driver");
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.error, None);
    assert_eq!(owned.close, uniplex.close);
}

#[test]
fn drain_from_a_write_completion_admits_no_read_ahead_request() {
    // Two keep-alive GETs in one read; the first response's write completion drains.
    for queued in [false, true] {
        let (owned, urls) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state, fence) = FencedTransport::new(
                    [ReadAction::Data(pipelined_gets(3)[..56].to_vec())],
                    queued,
                );
                fence.open.set(true);
                let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *fence.reentry.borrow_mut() = Some((
                    "write_complete",
                    Box::new(move || assert!(handle.initiate_drain().unwrap())),
                ));
                let result = session.run().await;
                fence.reentry.borrow_mut().take();
                let urls: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.url.borrow().clone())
                    .collect();
                (wire_of(&state, result), urls)
            })
        });
        assert_eq!(owned.error, None, "queued {queued}");
        assert_eq!(
            urls,
            [b"/0".to_vec()],
            "queued {queued}: only the first request is admitted"
        );
        let text = String::from_utf8_lossy(&owned.output);
        assert!(text.ends_with("\r\n\r\nhello"), "queued {queued}: {text:?}");
        assert_eq!(text.matches("HTTP/1.1 200").count(), 1, "queued {queued}");
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "queued {queued}");
    }
}

#[test]
fn head_204_and_304_responses_have_no_body() {
    const CLOSING_HEAD: &[u8] = b"HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    const CLOSING_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    // (request, response, whether the wire must end at the head). The last case is an
    // application that sends body bytes for HEAD: both sessions write what it sends.
    for (request, response, bodiless) in [
        (CLOSING_HEAD, Response::declaring(200, 5), true),
        (CLOSING_GET, Response::fixed(204, b""), true),
        (CLOSING_GET, Response::declaring(304, 5), true),
        (CLOSING_HEAD, Response::fixed(200, b"hello"), false),
    ] {
        let reads = || vec![ReadAction::Data(request.to_vec())];
        let label = format!(
            "{} {} {bodiless}",
            String::from_utf8_lossy(&request[..4]),
            response.status
        );
        let (owned, probes) = owned_wire(reads(), vec![response.clone()]);
        let uniplex = uniplex_wire(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(probes[0].completed, 1, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert!(
            !bodiless || owned.output.ends_with(b"\r\n\r\n"),
            "{label}: no body follows the head: {:?}",
            String::from_utf8_lossy(&owned.output)
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

#[test]
fn close_delimited_response_to_http10() {
    // Without a length, an HTTP/1.0 client can detect the body's end only at the close, keep-alive
    // or not.
    for request in [
        &b"GET / HTTP/1.0\r\nHost: a\r\n\r\n"[..],
        &b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n"[..],
    ] {
        let reads = || vec![ReadAction::Data(request.to_vec())];
        let responses = vec![Response {
            content_length: false,
            ..Response::fixed(200, b"hello")
        }];
        let label = String::from_utf8_lossy(request);
        let (owned, probes) = owned_wire(reads(), responses.clone());
        let uniplex = uniplex_wire(reads(), responses);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(probes[0].completed, 1, "{label}");
        let text = String::from_utf8_lossy(&owned.output);
        assert!(
            !text.contains("Content-Length") && !text.contains("Transfer-Encoding"),
            "{label}: the body is delimited by the close: {text:?}"
        );
        assert!(text.ends_with("\r\n\r\nhello"), "{label}: {text:?}");
        assert_eq!(
            owned.close,
            CloseKind::ShutdownWrite,
            "{label}: then the transport closes"
        );
        assert_eq!(text, String::from_utf8_lossy(&uniplex.output), "{label}");
        assert_eq!(owned.close, uniplex.close, "{label}");
    }
}

/// Yields until the transport has written something, then returns what it wrote.
async fn first_writes(context: &WorkerContext, state: &Rc<RefCell<TransportState>>) -> Vec<u8> {
    for _ in 0..64 {
        if !state.borrow().writes.is_empty() {
            break;
        }
        context.host().yield_to_io().await;
    }
    state.borrow().writes.concat()
}

/// A POST whose last 7 body bytes arrive only after the first response is written, then a
/// closing GET. Returns the wire, what was written before the rest of the body arrived, and
/// how many requests the application received.
fn early_response_run(owned: bool) -> (Wire, Vec<u8>, usize) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&release), b"defghij".to_vec()),
                ReadAction::Data(
                    b"GET /next HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
                ),
            ]);
            let responses = vec![Response::fixed(200, b"ok"), Response::fixed(200, b"next")];
            let releaser = async {
                let written = first_writes(&context, &state).await;
                release.notify_one();
                written
            };
            if owned {
                let (mut application, probes) = H1App::new(responses);
                application.respond_early = true;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let (result, written) = futures::join!(session.run(), releaser);
                let seen = probes.borrow().len();
                (wire_of(&state, result), written, seen)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early: true,
                    ..ScriptHandler::new(responses)
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let (result, written) = futures::join!(session.run(), releaser);
                (wire_of(&state, result), written, handler.served.get())
            }
        })
    })
}

#[test]
fn early_response_before_the_request_body_ends() {
    let (owned, owned_early, owned_seen) = early_response_run(true);
    let (uniplex, uniplex_early, uniplex_seen) = early_response_run(false);
    let early = String::from_utf8_lossy(&owned_early);
    assert!(
        early.ends_with("\r\n\r\nok"),
        "the response is written before the request body ends: {early:?}"
    );
    assert_eq!(early, String::from_utf8_lossy(&uniplex_early));
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        String::from_utf8_lossy(&uniplex.output)
    );
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
    assert_eq!(
        (owned_seen, uniplex_seen),
        (2, 2),
        "the unread body is drained and the connection serves the next request"
    );
}

#[test]
fn response_trailers_become_a_chunked_trailer_section() {
    let reads = || {
        vec![ReadAction::Data(
            b"GET /rpc HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec(),
        )]
    };
    let responses = vec![Response {
        content_length: false,
        trailers: vec![("grpc-status", "0")],
        ..Response::fixed(200, b"hello")
    }];
    let (owned, probes) = owned_wire(reads(), responses.clone());
    let uniplex = uniplex_wire(reads(), responses);
    assert_eq!(owned.error, None);
    assert_eq!(probes[0].completed, 1);
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.contains("Transfer-Encoding: chunked\r\n"), "{text:?}");
    assert!(
        text.ends_with("\r\n\r\n5\r\nhello\r\n0\r\ngrpc-status: 0\r\n\r\n"),
        "the body, then the trailer section ends the chunked body: {text:?}"
    );
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
}

/// An `Expect: 100-continue` POST whose body is sent only once the session wrote something.
/// Returns the wire and what was written before the body arrived.
fn expect_continue_run(owned: bool) -> (Wire, Vec<u8>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST /u HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\n"
                        .to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&release), b"hello".to_vec()),
            ]);
            let responses = vec![Response {
                interim: vec![100],
                ..Response::fixed(200, b"ok")
            }];
            let releaser = async {
                let written = first_writes(&context, &state).await;
                release.notify_one();
                written
            };
            let result = if owned {
                let (application, _probes) = H1App::new(responses);
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                futures::join!(session.run(), releaser)
            } else {
                let handler: HttpHandlerRef = Rc::new(ScriptHandler::new(responses));
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    h1_config(),
                )
                .unwrap();
                futures::join!(session.run(), releaser)
            };
            (wire_of(&state, result.0), result.1)
        })
    })
}

#[test]
fn expect_continue_matches_uniplex() {
    let (owned, owned_early) = expect_continue_run(true);
    let (uniplex, uniplex_early) = expect_continue_run(false);
    let early = String::from_utf8_lossy(&owned_early);
    assert_eq!(
        early, "HTTP/1.1 100 \r\nDate: Tue, 03 Nov 2026 00:00:00 GMT\r\n\r\n",
        "the interim response is written before the request body is sent"
    );
    assert_eq!(early, String::from_utf8_lossy(&uniplex_early));
    let text = String::from_utf8_lossy(&owned.output);
    assert!(text.ends_with("\r\n\r\nok"), "{text:?}");
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
}

const CONNECT_HEAD: &[u8] = b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n";
const WEBSOCKET_HEAD: &[u8] = b"GET /ws HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

/// Asserts that both applications observed the same requests.
fn assert_same_requests(owned: &[Observed], uniplex: &[Observed], label: &str) {
    let view = |observed: &[Observed]| {
        observed
            .iter()
            .map(|probe| {
                (
                    probe.method,
                    probe.url.clone(),
                    probe.fields.clone(),
                    probe.request_eom,
                    probe.request_body.clone(),
                    probe.request_trailers.clone(),
                    probe.request_finished,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        view(owned),
        view(uniplex),
        "{label}: the requests either application saw"
    );
}

#[test]
fn upgrade_under_proxygen_policy_matches_uniplex() {
    assert_eq!(
        Http1DriverConfig::default().upgrade_policy,
        Http1UpgradePolicy::Proxygen,
        "the default upgrade policy is Proxygen's, which these runs use"
    );
    for (head, response) in [
        (CONNECT_HEAD, Response::tunnel(200, &[b'y'; 100], false)),
        (WEBSOCKET_HEAD, Response::tunnel(200, &[b'y'; 100], true)),
    ] {
        let mut request = head.to_vec();
        request.extend_from_slice(&[b'x'; 100]);
        let reads = || vec![ReadAction::Data(request.clone()), ReadAction::Eof];
        let label = String::from_utf8_lossy(&head[..7]).into_owned();
        let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
        let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(
            owned_requests[0].request_body, [b'x'; 100],
            "{label}: the bytes after the head reach the exchange as its request body"
        );
        assert!(
            owned_requests[0].request_finished,
            "{label}: EOF ends that body"
        );
        assert!(
            owned.output.ends_with(&[b'y'; 100]),
            "{label}: the response body follows the accepting head unframed"
        );
        assert_same_requests(&owned_requests, &uniplex_requests, &label);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

#[test]
fn refused_upgrade_matches_uniplex() {
    // A rejected WebSocket upgrade with a request read behind it, and a rejected CONNECT.
    let mut websocket = WEBSOCKET_HEAD.to_vec();
    websocket.extend_from_slice(b"GET /later HTTP/1.1\r\nHost: a\r\n\r\n");
    for (request, status) in [(websocket, 400), (CONNECT_HEAD.to_vec(), 403)] {
        let reads = || vec![ReadAction::Data(request.clone())];
        let label = String::from_utf8_lossy(&request[..7]).into_owned();
        // The head is sent before the request body is read, and the rejection ends that body.
        let response = Response {
            tunnel: true,
            ..Response::fixed(status, b"no")
        };
        let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
        let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(
            owned_requests.len(),
            1,
            "{label}: nothing after the request is admitted"
        );
        assert!(owned_requests[0].request_finished, "{label}");
        assert_eq!(owned_requests[0].completed, 1, "{label}");
        assert!(owned.output.ends_with(b"\r\n\r\nno"), "{label}");
        assert!(
            String::from_utf8_lossy(&owned.output).contains("\r\nConnection: close\r\n"),
            "{label}: the codec made the connection non-reusable at the request head"
        );
        assert_eq!(owned.close, CloseKind::ShutdownWrite, "{label}");
        assert_same_requests(&owned_requests, &uniplex_requests, &label);
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&uniplex.output),
            "{label}"
        );
        assert_eq!(owned.close, uniplex.close, "{label}");
        assert_eq!(owned.error, uniplex.error, "{label}");
    }
}

/// A 1xx before the final response to a downstream CONNECT does not resolve the upgrade, in
/// the owned driver or in `UniplexSession`. Only the final head does, so a 1xx and then a 2xx
/// leave the tunnel working.
#[test]
fn interim_before_upgrade_final_is_pinned() {
    let mut request = CONNECT_HEAD.to_vec();
    request.extend_from_slice(&[b'x'; 10]);
    let reads = || vec![ReadAction::Data(request.clone()), ReadAction::Eof];
    let response = Response {
        interim: vec![100],
        ..Response::tunnel(200, &[b'y'; 10], false)
    };
    let (owned, owned_requests) = owned_wire(reads(), vec![response.clone()]);
    let (uniplex, uniplex_requests) = uniplex_wire_observed(reads(), vec![response]);
    assert_eq!(owned.error, None);
    assert_eq!(
        owned_requests[0].request_body, [b'x'; 10],
        "the bytes after the CONNECT head still reach the exchange as tunnel bytes"
    );
    let text = String::from_utf8_lossy(&owned.output);
    assert!(
        text.starts_with("HTTP/1.1 100 \r\n"),
        "the interim head is written first: {text:?}"
    );
    assert!(text.ends_with("yyyyyyyyyy"), "{text:?}");
    assert_same_requests(&owned_requests, &uniplex_requests, "CONNECT");
    assert_eq!(text, String::from_utf8_lossy(&uniplex.output));
    assert_eq!(owned.close, uniplex.close);
    assert_eq!(owned.error, uniplex.error);
}

/// What one fenced retirement run observed: the response-complete and complete counts while
/// the response's write was still held, the counts after the run, the saturation queries,
/// and the wire.
struct FencedRetirement {
    held: (usize, usize),
    completed: usize,
    queries: usize,
    wire: Wire,
}

/// Serves `CLOSING_GET` through a write queue whose completions are held and whose
/// `is_saturated` returns `!unsaturated`. `instrument` sets `H1App::instrument`.
fn fenced_retirement(unsaturated: bool, instrument: bool) -> FencedRetirement {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())], true);
            fence.unsaturated.set(unsaturated);
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.instrument = instrument;
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let release = async {
                for _ in 0..64 {
                    if !state.borrow().writes.is_empty() {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                // Yields for the driver turns in which it could retire the exchange.
                for _ in 0..8 {
                    context.host().yield_to_io().await;
                }
                let probe = Rc::clone(&probes.borrow()[0]);
                let held = (probe.response_complete.get(), probe.completed.get());
                fence.open();
                held
            };
            let (result, held) = futures::join!(session.run(), release);
            let completed = probes.borrow()[0].completed.get();
            FencedRetirement {
                held,
                completed,
                queries: fence.saturation_queries.get(),
                wire: wire_of(&state, result),
            }
        })
    })
}

#[test]
fn unsaturated_h1_stream_retires_at_serialization() {
    let run = fenced_retirement(true, false);
    assert_eq!(
        run.held,
        (1, 1),
        "an unsaturated queue lets the exchange retire before its write completes"
    );
    assert_eq!(run.completed, 1, "it retires once");
    assert_eq!(
        run.queries, 1,
        "the queue is asked once, at END_STREAM serialization"
    );
    assert_eq!(run.wire.error, None);
    assert!(run.wire.output.ends_with(b"\r\n\r\nhello"));
    assert_eq!(
        run.wire.close,
        CloseKind::ShutdownWrite,
        "the connection still closes only after its write completes"
    );
}

#[test]
fn saturated_h1_stream_keeps_its_write_fence() {
    for (label, unsaturated, instrument, queries) in [
        ("a saturated queue", false, false, 1),
        ("a byte-event registration", true, true, 0),
    ] {
        let run = fenced_retirement(unsaturated, instrument);
        assert_eq!(
            run.held,
            (1, 0),
            "{label}: the exchange keeps its write fence"
        );
        assert_eq!(
            run.completed, 1,
            "{label}: it retires once its write completes"
        );
        assert_eq!(
            run.queries, queries,
            "{label}: only an otherwise eligible exchange asks the queue"
        );
        assert_eq!(run.wire.error, None, "{label}");
        assert_eq!(run.wire.close, CloseKind::ShutdownWrite, "{label}");
    }
}

/// Downstream HTTP/1.1 ingress artifacts, each a name and the fragments its input arrives in.
/// Do not change them: `ingress_artifacts_keep_their_bytes` checks their lengths and hashes.
const COPIED_FROZEN_ARTIFACTS: &[(&str, &[&[u8]])] = &[
    (
        "fragmented_chunked_request_with_trailer",
        &[
            b"POST /resource?x=1 HTTP/1.1\r\nHost: example.com\r\n",
            b"Transfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n",
            b"0\r\nX-Trailer: done\r\n\r\n",
        ],
    ),
    (
        "pipelined_requests",
        &[b"GET /one HTTP/1.1\r\n\r\nGET /two HTTP/1.1\r\n\r\n"],
    ),
    (
        "malformed_header",
        &[b"GET /bad HTTP/1.1\r\nMalformed\r\n\r\n"],
    ),
];

#[test]
fn ingress_artifacts_keep_their_bytes() {
    // Each artifact's fragment lengths and the FNV-1a 64 hash of its bytes.
    let expected: [(&str, &[usize], u64); 3] = [
        (
            "fragmented_chunked_request_with_trailer",
            &[48, 39, 22],
            0x7e66_e9ab_c18e_df79,
        ),
        ("pipelined_requests", &[42], 0xae81_04a6_2eb2_6587),
        ("malformed_header", &[32], 0xaf05_6c6a_4ad6_69f6),
    ];
    let fnv1a = |fragments: &[&[u8]]| {
        fragments
            .iter()
            .flat_map(|fragment| fragment.iter())
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    };
    let copied: Vec<_> = COPIED_FROZEN_ARTIFACTS
        .iter()
        .map(|(name, fragments)| {
            let lengths: Vec<usize> = fragments.iter().map(|part| part.len()).collect();
            (*name, lengths, fnv1a(fragments))
        })
        .collect();
    let expected: Vec<_> = expected
        .iter()
        .map(|(name, lengths, hash)| (*name, lengths.to_vec(), *hash))
        .collect();
    assert_eq!(copied, expected);
}

/// One corpus vector: the input bytes, and the script both applications replay.
struct Vector {
    name: String,
    input: Vec<u8>,
    responses: Vec<Response>,
}

impl Vector {
    fn new(name: &str, input: &[u8], responses: Vec<Response>) -> Self {
        Self {
            name: name.to_owned(),
            input: input.to_vec(),
            responses,
        }
    }
}

/// How a vector's input is cut into reads.
#[derive(Clone, Copy, Debug)]
enum Split {
    Whole,
    Every7,
    OneByte,
}

impl Split {
    fn reads(self, input: &[u8]) -> Vec<Vec<u8>> {
        match self {
            Self::Whole => vec![input.to_vec()],
            Self::Every7 => input.chunks(7).map(<[u8]>::to_vec).collect(),
            Self::OneByte => input.chunks(1).map(<[u8]>::to_vec).collect(),
        }
    }
}

/// A `POST /generate` shaped like real traffic: 18 header lines and a 766-byte head, with
/// typical field lengths (a 14-byte host, a 53-byte request ID, a 41-byte routing key and its
/// copy, filler to the total) and generic names.
fn consumer_shaped_post() -> Vec<u8> {
    let body_length = 1536;
    let mut lines = vec![
        "host: c1.example.net".to_owned(),
        "content-type: application/json".to_owned(),
        "accept: text/event-stream".to_owned(),
        format!("content-length: {body_length}"),
        format!("x-request-id: {}", "r".repeat(53)),
        format!("x-routing-key: {}", "k".repeat(41)),
        format!("x-routing-key-copy: {}", "k".repeat(41)),
        "x-timeout-ms: 3600000".to_owned(),
    ];
    let start = "POST /generate HTTP/1.1\r\n";
    let fixed: usize = start.len() + lines.iter().map(|line| line.len() + 2).sum::<usize>() + 2;
    let fillers = 18 - lines.len();
    // Each filler line is `x-pad-req-N: <value>\r\n`.
    let mut remaining = 766 - fixed;
    for index in 0..fillers {
        let name = format!("x-pad-req-{index}: ");
        let left = fillers - index;
        let line = remaining / left;
        remaining -= line;
        lines.push(format!("{name}{}", "p".repeat(line - name.len() - 2)));
    }
    let mut head = start.to_owned();
    for line in &lines {
        head.push_str(line);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    assert_eq!(head.len(), 766, "the consumer-shaped head is 766 bytes");
    assert_eq!(lines.len(), 18, "of 18 header lines");
    let mut request = head.into_bytes();
    request.extend(std::iter::repeat_n(b'j', body_length));
    request
}

/// A GET whose head is `length` bytes, padded by one filler field.
fn head_of_length(length: usize) -> Vec<u8> {
    let start = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\nx-pad-req: ";
    let end = b"\r\n\r\n";
    let mut head = start.to_vec();
    head.extend(std::iter::repeat_n(b'p', length - start.len() - end.len()));
    head.extend_from_slice(end);
    head
}

fn corpus() -> Vec<Vector> {
    let ok = || vec![Response::fixed(200, b"ok")];
    let mut vectors: Vec<Vector> = COPIED_FROZEN_ARTIFACTS
        .iter()
        .map(|(name, fragments)| Vector::new(name, &fragments.concat(), ok()))
        .collect();
    let event_stream = Response {
        content_length: false,
        ..Response::fixed(200, b"data: one\n\ndata: two\n\n")
    };
    vectors.extend([
        Vector::new("consumer_shaped_post", &consumer_shaped_post(), vec![event_stream.clone()]),
        Vector::new(
            "event_stream_response",
            b"GET /events HTTP/1.1\r\nHost: a\r\nAccept: text/event-stream\r\n\r\n",
            vec![event_stream],
        ),
        Vector::new(
            "head",
            b"HEAD / HTTP/1.1\r\nHost: a\r\n\r\n",
            vec![Response::declaring(200, 5)],
        ),
        Vector::new("no_content", b"GET / HTTP/1.1\r\nHost: a\r\n\r\n", vec![Response::fixed(204, b"")]),
        Vector::new(
            "not_modified",
            b"GET / HTTP/1.1\r\nHost: a\r\nIf-None-Match: \"v1\"\r\n\r\n",
            vec![Response::declaring(304, 5)],
        ),
        Vector::new("http10", b"GET / HTTP/1.0\r\nHost: a\r\n\r\n", ok()),
        Vector::new(
            "http10_keep_alive",
            b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "connection_close",
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "two_pipelined_requests",
            b"GET /0 HTTP/1.1\r\nHost: a\r\n\r\nPOST /1 HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc",
            vec![Response::fixed(200, b"zero"), Response::fixed(200, b"one")],
        ),
        Vector::new(
            "content_length_and_chunked",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "duplicate_content_length",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabcd",
            ok(),
        ),
        Vector::new(
            "obs_fold",
            b"GET / HTTP/1.1\r\nHost: a\r\nX-Folded: one\r\n two\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "absolute_form_with_host",
            b"GET http://a.example/path?q=1 HTTP/1.1\r\nHost: b.example\r\n\r\n",
            ok(),
        ),
        Vector::new(
            "connect",
            &[CONNECT_HEAD, b"tunnel bytes"].concat(),
            vec![Response::tunnel(200, b"tunnel reply", false)],
        ),
        Vector::new(
            "websocket_upgrade",
            &[WEBSOCKET_HEAD, b"frame bytes"].concat(),
            vec![Response::tunnel(200, b"frame reply", true)],
        ),
        Vector::new(
            "expect_continue",
            b"POST /u HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello",
            vec![Response {
                interim: vec![100],
                ..Response::fixed(200, b"ok")
            }],
        ),
        Vector::new(
            "chunked_with_trailers",
            b"POST /c HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nx-checksum: 1\r\n\r\n",
            ok(),
        ),
        Vector::new("head_at_the_limit", &head_of_length(80 << 10), ok()),
        Vector::new("head_over_the_limit", &head_of_length((80 << 10) + 1), ok()),
        Vector::new("malformed_request_line", b"GET/ HTTP/1.1\r\nHost: a\r\n\r\n", ok()),
    ]);
    vectors
}

/// Replaces every `Date` value with one placeholder, since error responses take it from the
/// clock.
fn without_dates(output: &[u8]) -> String {
    String::from_utf8_lossy(output)
        .split("\r\n")
        .map(|line| {
            if line.starts_with("Date: ") {
                "Date: <date>"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// The cause an application was given for an exchange that did not finish: the cancel cause
/// (owned) or the error a request read returned (Uniplex).
fn cause(probe: &Observed) -> Option<HttpErrorCode> {
    probe
        .cancelled
        .first()
        .map(|(code, _)| *code)
        .or(probe.request_error)
}

/// What one corpus run produced, in comparable form.
#[derive(Debug, PartialEq)]
struct CorpusRun {
    output: String,
    close: CloseKind,
    error: Option<HttpErrorCode>,
    requests: Vec<(
        Option<HttpMethod>,
        Vec<u8>,
        Vec<(Vec<u8>, Vec<u8>)>,
        bool,
        Vec<u8>,
        Vec<(Vec<(Vec<u8>, Vec<u8>)>, bool)>,
        bool,
        Option<HttpErrorCode>,
    )>,
}

/// Runs `vector` cut by `split` through one session. Peer EOF follows the input once the
/// session has read all of it and gone idle, so EOF cannot race a response.
fn corpus_run(owned: bool, vector: &Vector, split: Split) -> CorpusRun {
    corpus_run_configured(owned, vector, split, Http1DriverConfig::default())
}

/// As `corpus_run`, with the owned driver's HTTP/1.1 configuration `http1`.
fn corpus_run_configured(
    owned: bool,
    vector: &Vector,
    split: Split,
    http1: Http1DriverConfig,
) -> CorpusRun {
    corpus_run_with_io(owned, vector, split, http1, h1_config()).0
}

/// As `corpus_run_configured`, with both sessions under the I/O configuration `io`; also
/// returns each request's method as the application read it (`HttpMessage::request_method`).
fn corpus_run_with_io(
    owned: bool,
    vector: &Vector,
    split: Split,
    http1: Http1DriverConfig,
    io: UniplexConfig,
) -> (CorpusRun, Vec<Vec<u8>>) {
    let (wire, observed) = corpus_session(owned, vector, split, http1, io);
    let run = CorpusRun {
        output: without_dates(&wire.output),
        close: wire.close,
        error: wire.error,
        requests: observed
            .iter()
            .map(|probe| {
                (
                    probe.method,
                    probe.url.clone(),
                    probe.fields.clone(),
                    probe.request_eom,
                    probe.request_body.clone(),
                    probe.request_trailers.clone(),
                    probe.request_finished,
                    cause(probe),
                )
            })
            .collect(),
    };
    let methods = observed
        .iter()
        .map(|probe| probe.request_method.clone())
        .collect();
    (run, methods)
}

/// Runs `vector` cut by `split` through one session, as `corpus_run_with_io` does; returns its
/// wire and what each application observed.
fn corpus_session(
    owned: bool,
    vector: &Vector,
    split: Split,
    http1: Http1DriverConfig,
    io: UniplexConfig,
) -> (Wire, Vec<Observed>) {
    let reads = split.reads(&vector.input);
    let responses = vector.responses.clone();
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(
                reads
                    .into_iter()
                    .map(ReadAction::Data)
                    .chain([ReadAction::WaitEof(Arc::clone(&release))]),
            );
            let done = Cell::new(false);
            let releaser = async {
                let mut settled = 0;
                while !done.get() && settled < 64 {
                    if state.borrow().reads.is_empty() {
                        settled += 1;
                    }
                    context.host().yield_to_io().await;
                }
                release.notify_one();
            };
            if owned {
                let (application, probes) = H1App::new(responses);
                let mut options = owned_options(context.clone(), transport);
                options.http1 = http1;
                options.io = io;
                let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                let (result, ()) = futures::join!(run, releaser);
                let observed: Vec<_> = probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            } else {
                let handler = Rc::new(ScriptHandler::new(responses));
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    io,
                )
                .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                let (result, ()) = futures::join!(run, releaser);
                let observed: Vec<_> = handler
                    .probes
                    .borrow()
                    .iter()
                    .map(|probe| probe.observed())
                    .collect();
                (wire_of(&state, result), observed)
            }
        })
    })
}

/// `h1_config` with `exact_method_tokens` set.
fn exact_methods_config() -> UniplexConfig {
    UniplexConfig {
        exact_method_tokens: true,
        ..h1_config()
    }
}

/// A vector of one bodiless request whose method is `method`, with a 200 response.
fn method_vector(method: &[u8]) -> Vector {
    Vector::new(
        &String::from_utf8_lossy(method),
        &[
            method,
            b" /generate HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n",
        ]
        .concat(),
        vec![Response::fixed(200, b"ok")],
    )
}

/// Both drivers preserve llhttp-recognized method tokens across read boundaries.
#[test]
fn llhttp_methods_reach_the_exchange_as_sent_as_uniplex() {
    for method in [&b"POST"[..], b"PURGE", b"M-SEARCH"] {
        let vector = method_vector(method);
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{} {split:?}", vector.name);
            let run = |owned| {
                corpus_run_with_io(
                    owned,
                    &vector,
                    split,
                    Http1DriverConfig::default(),
                    exact_methods_config(),
                )
            };
            let (owned, owned_methods) = run(true);
            let (uniplex, uniplex_methods) = run(false);
            assert_eq!(owned_methods, [method], "{label}: the method as sent");
            assert_eq!(
                owned.requests[0].0,
                (method == b"POST").then_some(HttpMethod::Post),
                "{label}: only the canonical spelling is recognized"
            );
            assert!(
                owned.output.starts_with("HTTP/1.1 200 "),
                "{label}: {}",
                owned.output
            );
            assert_eq!(owned_methods, uniplex_methods, "{label}");
            assert_eq!(owned, uniplex, "{label}");
        }
    }
}

/// The HTTP/2 method option does not broaden HTTP/1 grammar. Unsupported or
/// malformed method tokens fail before an exchange is delivered.
#[test]
fn http2_method_option_does_not_broaden_http1_methods() {
    for method in [
        &b"P@ST"[..],
        b"(GET)",
        b"PO\x7fST",
        b"post",
        b"Post",
        b"CUSTOM",
    ] {
        let vector = method_vector(method);
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{} {split:?}", vector.name.escape_debug());
            let run = |owned| {
                corpus_run_with_io(
                    owned,
                    &vector,
                    split,
                    Http1DriverConfig::default(),
                    exact_methods_config(),
                )
            };
            let (owned, owned_methods) = run(true);
            let (uniplex, uniplex_methods) = run(false);
            assert!(
                owned.output.starts_with("HTTP/1.1 400 Bad Request\r\n"),
                "{label}: {}",
                owned.output
            );
            assert_eq!(owned.close, CloseKind::Reset, "{label}");
            assert!(
                owned_methods.iter().all(Vec::is_empty),
                "{label}: no request reaches an exchange: {owned_methods:?}"
            );
            assert_eq!(owned_methods, uniplex_methods, "{label}");
            assert_eq!(owned, uniplex, "{label}");
        }
    }
}

/// Unsupported and incorrectly cased methods fail under both drivers.
#[test]
fn methods_outside_llhttp_table_get_400_by_default_as_uniplex() {
    for method in [&b"post"[..], b"Post", b"CUSTOM"] {
        let vector = method_vector(method);
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{} {split:?}", vector.name);
            let run = |owned| {
                corpus_run_with_io(
                    owned,
                    &vector,
                    split,
                    Http1DriverConfig::default(),
                    h1_config(),
                )
            };
            let (owned, owned_methods) = run(true);
            let (uniplex, uniplex_methods) = run(false);
            assert!(
                owned.output.starts_with("HTTP/1.1 400 Bad Request\r\n"),
                "{label}: {}",
                owned.output
            );
            assert!(
                owned_methods.iter().all(Vec::is_empty),
                "{label}: no request reaches an exchange: {owned_methods:?}"
            );
            assert_eq!(owned_methods, uniplex_methods, "{label}");
            assert_eq!(owned, uniplex, "{label}");
        }
    }
}

/// Both drivers respond to a rejected head with the codec's status and close: 414 for a
/// request-target past the head limit, 431 for another head past it, 501 for a transfer coding
/// under a final chunked, and 400 when chunked is not last.
#[test]
fn rejected_heads_get_the_codec_status_as_uniplex() {
    let mut long_target = b"GET /".to_vec();
    long_target.resize((80 << 10) + 1, b'a');
    long_target.extend_from_slice(b" HTTP/1.1\r\nHost: a\r\n\r\n");
    let coded = |codings: &str| {
        format!("POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: {codings}\r\n\r\n0\r\n\r\n")
            .into_bytes()
    };
    for (name, input, status) in [
        ("long_target", long_target, "414 Request-URI Too Long"),
        (
            "head_over_the_limit",
            head_of_length((80 << 10) + 1),
            "431 Request Header Fields Too Large",
        ),
        (
            "gzip_chunked",
            coded("gzip, chunked"),
            "501 Not Implemented",
        ),
        ("chunked_gzip", coded("chunked, gzip"), "400 Bad Request"),
    ] {
        let vector = Vector::new(name, &input, vec![Response::fixed(200, b"ok")]);
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{name} {split:?}");
            let owned = corpus_run(true, &vector, split);
            let uniplex = corpus_run(false, &vector, split);
            assert_eq!(
                owned.output,
                format!(
                    "HTTP/1.1 {status}\r\nDate: <date>\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
                ),
                "{label}"
            );
            assert_eq!(owned.close, CloseKind::Reset, "{label}");
            assert_eq!(owned, uniplex, "{label}");
        }
    }
}

/// Every corpus vector, fed whole, in 7-byte reads and one byte at a time, writes the same
/// bytes (dates aside) and shows the application the same events under both sessions.
#[test]
fn owned_h1_matches_uniplex_on_the_corpus() {
    let mut differences = Vec::new();
    for vector in corpus() {
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let owned = corpus_run(true, &vector, split);
            let uniplex = corpus_run(false, &vector, split);
            if owned != uniplex {
                differences.push(format!(
                    "{} {split:?}:\n  owned:   {owned:?}\n  uniplex: {uniplex:?}",
                    vector.name
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

fn ordinary_request_policy() -> Http1DriverConfig {
    Http1DriverConfig {
        upgrade_policy: Http1UpgradePolicy::AsOrdinaryRequest,
        ..Http1DriverConfig::default()
    }
}

/// The header fields of a request head as it reads on the wire.
fn wire_fields(head: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    String::from_utf8_lossy(head)
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.as_bytes().to_vec(), value.trim().as_bytes().to_vec())
        })
        .collect()
}

/// Under `AsOrdinaryRequest`, a WebSocket upgrade request and a CONNECT request are handed
/// over as ordinary requests. Parsing does not pause for an upgrade, the request behind each is
/// parsed and served on the same connection, every response is framed, and neither carries a
/// tunnel. The wire equals the default policy's for two ordinary GETs with the same responses.
#[test]
fn upgrade_as_ordinary_request_keeps_parsing_http() {
    const LATER: &[u8] = b"GET /later HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    let responses = || vec![Response::fixed(200, b"one"), Response::fixed(200, b"two")];
    let ordinary = [b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), LATER].concat();
    let (reference, reference_requests) = owned_wire(vec![ReadAction::Data(ordinary)], responses());
    assert_eq!(reference_requests.len(), 2);
    assert_eq!(reference.error, None);
    let reference_output = String::from_utf8_lossy(&reference.output).into_owned();
    assert!(
        reference_output.ends_with("\r\n\r\ntwo"),
        "{reference_output}"
    );
    assert_eq!(reference_output.matches("Content-Length: 3\r\n").count(), 2);
    for (head, method, url) in [
        (WEBSOCKET_HEAD, HttpMethod::Get, b"/ws".as_slice()),
        (CONNECT_HEAD, HttpMethod::Connect, b"h:443".as_slice()),
    ] {
        let label = String::from_utf8_lossy(&head[..7]).into_owned();
        let reads = vec![ReadAction::Data([head, LATER].concat())];
        let (owned, requests) =
            owned_wire_configured(reads, responses(), false, ordinary_request_policy());
        assert_eq!(owned.error, None, "{label}");
        assert_eq!(requests.len(), 2, "{label}: the request behind is parsed");
        assert_eq!(requests[0].method, Some(method), "{label}");
        assert_eq!(requests[0].url, url, "{label}");
        assert_eq!(
            requests[0].fields,
            wire_fields(head),
            "{label}: headers unchanged"
        );
        assert!(
            requests[0].request_body.is_empty(),
            "{label}: no tunnel bytes"
        );
        assert!(requests[0].request_finished, "{label}");
        assert_eq!(requests[1].url, b"/later", "{label}");
        assert!(requests[1].request_finished, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&owned.output),
            String::from_utf8_lossy(&reference.output),
            "{label}: both responses framed, on one connection"
        );
        assert_eq!(owned.close, reference.close, "{label}");
    }
}

/// The corpus's `connect` and `websocket_upgrade` vectors under `AsOrdinaryRequest`, every
/// split. The head reaches the exchange as an ordinary request, and the bytes behind it are
/// parsed as the next request, which fails as a malformed start line with the error response
/// and close of the corpus's `malformed_request_line` vector. Apart from the head, the run
/// equals the default policy's on the same bytes behind an ordinary GET head, with the same
/// response. With the default policy the vectors still equal `UniplexSession`'s.
#[test]
fn owned_h1_corpus_upgrade_vectors_parse_as_ordinary_requests() {
    const ORDINARY_HEAD: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
    let corpus = corpus();
    let malformed = corpus
        .iter()
        .find(|vector| vector.name == "malformed_request_line")
        .unwrap();
    for vector in corpus
        .iter()
        .filter(|vector| matches!(vector.name.as_str(), "connect" | "websocket_upgrade"))
    {
        let head = if vector.name == "connect" {
            CONNECT_HEAD
        } else {
            WEBSOCKET_HEAD
        };
        let behind = &vector.input[head.len()..];
        let option_vector = Vector::new(
            &vector.name,
            &vector.input,
            vec![Response::fixed(200, b"ok")],
        );
        let ordinary_vector = Vector::new(
            "ordinary",
            &[ORDINARY_HEAD, behind].concat(),
            vec![Response::fixed(200, b"ok")],
        );
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{} {split:?}", vector.name);
            let mut option =
                corpus_run_configured(true, &option_vector, split, ordinary_request_policy());
            assert_eq!(
                option.requests.len(),
                2,
                "{label}: the bytes behind are a request"
            );
            let first = &mut option.requests[0];
            assert_eq!(first.2, wire_fields(head), "{label}: headers unchanged");
            assert!(
                first.4.is_empty(),
                "{label}: the bytes behind are not its body"
            );
            assert!(first.6, "{label}: the request is finished");
            let refused = corpus_run(true, malformed, split);
            assert!(
                option.output.ends_with(&refused.output),
                "{label}: {:?} ends with the malformed start line's {:?}",
                option.output,
                refused.output
            );
            assert_eq!(option.close, refused.close, "{label}");
            assert_eq!(option.error, refused.error, "{label}");
            assert_eq!(option.requests[1].7, refused.requests[0].7, "{label}");
            // Apart from the head itself, the run is the ordinary request's.
            let ordinary = corpus_run(true, &ordinary_vector, split);
            let reference_head = &ordinary.requests[0];
            let first = &mut option.requests[0];
            first.0 = reference_head.0;
            first.1 = reference_head.1.clone();
            first.2 = reference_head.2.clone();
            assert_eq!(option, ordinary, "{label}");
            assert_eq!(
                corpus_run(true, vector, split),
                corpus_run(false, vector, split),
                "{label}: default policy"
            );
        }
    }
}

/// Serves `request` then a peer EOF, in two reads, with `abort_exchanges_on_eof` set to
/// `abort`, on the owned driver and on `UniplexSession`.
fn eof_abort_runs(
    abort: bool,
    request: &[u8],
    response: Response,
) -> ((Wire, Vec<Observed>), (Wire, Vec<Observed>)) {
    let io = UniplexConfig {
        abort_exchanges_on_eof: abort,
        ..h1_config()
    };
    let reads = || vec![ReadAction::Data(request.to_vec()), ReadAction::Eof];
    (
        configured_wire(true, reads(), response.clone(), io),
        configured_wire(false, reads(), response, io),
    )
}

/// The owned driver handles a peer EOF as `UniplexSession` does. Each script gives the same
/// wire, close and error under both sessions. Where the EOF aborts an exchange, the owned
/// application gets one cancel, attributed to the peer.
#[test]
fn eof_abort_matches_uniplex() {
    let mut connect = CONNECT_HEAD.to_vec();
    connect.extend_from_slice(&[b'x'; 100]);
    let mut websocket = WEBSOCKET_HEAD.to_vec();
    websocket.extend_from_slice(&[b'x'; 100]);
    let upload = b"POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc".to_vec();
    let refused = Response {
        tunnel: true,
        ..Response::fixed(400, b"no")
    };
    // (label, option on, request, response, whether the EOF aborts the exchange)
    let cases = [
        // By default the codec responds to the cut-short upload with 400, then resets. With
        // the option, the open exchange is aborted first, without a response.
        (
            "mid-upload, off",
            false,
            upload.clone(),
            Response::fixed(200, b"hello"),
            false,
        ),
        (
            "mid-upload",
            true,
            upload,
            Response::fixed(200, b"hello"),
            true,
        ),
        // An EOF that ends an accepted tunnel's ingress does not abort it. The tunnel's
        // response follows.
        (
            "CONNECT tunnel",
            true,
            connect,
            Response::tunnel(200, &[b'y'; 100], false),
            false,
        ),
        (
            "WebSocket tunnel",
            true,
            websocket,
            Response::tunnel(200, &[b'y'; 100], true),
            false,
        ),
        // A rejected upgrade's response head is serialized behind the pending upgrade before
        // the EOF is handled, but its body is not. The abort writes the head, then resets.
        (
            "refused upgrade",
            true,
            WEBSOCKET_HEAD.to_vec(),
            refused,
            true,
        ),
        // The request reaches the application, whose ready response is not written.
        (
            "request and EOF",
            true,
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
            Response::fixed(200, b"hello"),
            true,
        ),
    ];
    for (label, abort, request, response, aborted) in cases {
        let ((owned, owned_probes), (uniplex, _)) = eof_abort_runs(abort, &request, response);
        assert_eq!(
            without_dates(&owned.output),
            without_dates(&uniplex.output),
            "{label}: the wire"
        );
        assert_eq!(owned.close, uniplex.close, "{label}: the close");
        assert_eq!(owned.error, uniplex.error, "{label}: the error");
        if aborted {
            assert_eq!(
                (owned.close, owned.error),
                (CloseKind::Reset, Some(HttpErrorCode::TRANSPORT_EOF)),
                "{label}"
            );
            assert_eq!(
                owned_probes.last().map(|probe| probe.cancelled.clone()),
                Some(vec![(HttpErrorCode::TRANSPORT_EOF, true)]),
                "{label}: the exchange is cancelled once, as the peer's"
            );
        }
    }
}

/// Two pipelined requests, then a peer EOF. The EOF waits behind the second request, so the
/// first gets its response and the second is aborted, as with `UniplexSession`. The owned
/// driver reads the EOF while the first exchange is live, so that response already carries
/// `Connection: close`, whether or not the option is set. `UniplexSession` reads it later.
#[test]
fn eof_behind_a_pipelined_request_answers_the_first_and_aborts_the_second_as_uniplex() {
    let get = b"GET / HTTP/1.1\r\nHost: a\r\n\r\n";
    let ((owned, owned_probes), (uniplex, uniplex_probes)) = eof_abort_runs(
        true,
        &[get.as_slice(), get].concat(),
        Response::fixed(200, b"hello"),
    );
    let answered = |wire: &Wire| {
        let text = String::from_utf8_lossy(&wire.output).into_owned();
        (
            text.matches("HTTP/1.1 200").count(),
            text.ends_with("\r\n\r\nhello"),
        )
    };
    assert_eq!(answered(&owned), (1, true));
    assert_eq!(answered(&owned), answered(&uniplex));
    assert_eq!(
        (owned.close, owned.error),
        (CloseKind::Reset, Some(HttpErrorCode::TRANSPORT_EOF))
    );
    assert_eq!((owned.close, owned.error), (uniplex.close, uniplex.error));
    assert_eq!((owned_probes.len(), uniplex_probes.len()), (2, 2));
    assert_eq!(
        owned_probes
            .iter()
            .map(|probe| probe.cancelled.clone())
            .collect::<Vec<_>>(),
        [vec![], vec![(HttpErrorCode::TRANSPORT_EOF, true)]],
        "only the second exchange is cancelled, as the peer's"
    );
}

#[test]
fn malformed_head_behind_a_request_fails_as_uniplex() {
    const OK: &str =
        "HTTP/1.1 200 \r\nDate: <date>\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok";
    const BAD_REQUEST: &str = "HTTP/1.1 400 Bad Request\r\nDate: <date>\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
    for behind in [
        b"tunnel bytes".as_slice(),
        b"GET/ HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(),
    ] {
        let vector = Vector::new(
            "malformed_behind",
            &[b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".as_slice(), behind].concat(),
            vec![Response::fixed(200, b"ok")],
        );
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!(
                "{} {split:?}",
                String::from_utf8_lossy(behind).escape_debug()
            );
            let owned = corpus_run(true, &vector, split);
            let uniplex = corpus_run(false, &vector, split);
            assert_eq!(owned.requests, uniplex.requests, "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
            assert_eq!(owned.output, format!("{OK}{BAD_REQUEST}"), "{label}: owned");
            assert_eq!(owned.close, CloseKind::Reset, "{label}: owned");
            assert_eq!(uniplex, owned, "{label}");
        }
    }
}

/// Serves `reads`, then peer EOF once the session has read them all and gone idle (as
/// `corpus_run`), with the owned driver (`owned`) or `UniplexSession`. Each application drops
/// its request body unread and responds immediately.
fn dropping_bodies_wire(
    owned: bool,
    reads: Vec<Vec<u8>>,
    responses: Vec<Response>,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let release = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(
                reads
                    .into_iter()
                    .map(ReadAction::Data)
                    .chain([ReadAction::WaitEof(Arc::clone(&release))]),
            );
            let done = Cell::new(false);
            let releaser = async {
                let mut settled = 0;
                while !done.get() && settled < 64 {
                    if state.borrow().reads.is_empty() {
                        settled += 1;
                    }
                    context.host().yield_to_io().await;
                }
                release.notify_one();
            };
            let (result, probes) = if owned {
                let (mut application, probes) = H1App::new(responses);
                application.respond_early = true;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context.clone(), transport))
                        .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                (futures::join!(run, releaser).0, probes)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early: true,
                    ..ScriptHandler::new(responses)
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                let run = async {
                    let result = session.run().await;
                    done.set(true);
                    result
                };
                (futures::join!(run, releaser).0, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// A chunked request whose trailer section ends it retires once that section is read, when
/// its exchange drops the body unread, however the input is cut, and a request behind it on
/// the connection is served; both sessions write the same bytes.
#[test]
fn a_dropped_request_body_with_trailers_retires() {
    const TRAILERS: &[u8] =
        b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nx: v\r\n\r\n";
    const BEHIND: &[u8] = b"GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";
    for (input, requests) in [(TRAILERS.to_vec(), 1), ([TRAILERS, BEHIND].concat(), 2)] {
        for split in [Split::Whole, Split::Every7, Split::OneByte] {
            let label = format!("{requests} request(s), {split:?}");
            let responses = vec![Response::fixed(200, b"ok")];
            let (owned, probes) =
                dropping_bodies_wire(true, split.reads(&input), responses.clone());
            let (uniplex, _) = dropping_bodies_wire(false, split.reads(&input), responses);
            assert_eq!(probes.len(), requests, "{label}");
            for probe in &probes {
                assert_eq!(probe.response_complete, 1, "{label}");
                assert_eq!(probe.completed, 1, "{label}: the exchange retires once");
                assert_eq!(probe.dropped, 1, "{label}");
                assert!(probe.cancelled.is_empty(), "{label}");
            }
            let output = String::from_utf8_lossy(&owned.output);
            assert_eq!(output.matches("HTTP/1.1 200 ").count(), requests, "{label}");
            assert_eq!(output, String::from_utf8_lossy(&uniplex.output), "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
            assert_eq!(owned.close, uniplex.close, "{label}");
        }
    }
}

/// Serves `reads` with the owned driver (`owned`) or `UniplexSession`. With `respond_early`,
/// each application responds immediately and drops its request body unread.
fn early_wire(
    owned: bool,
    reads: Vec<ReadAction>,
    response: Response,
    respond_early: bool,
) -> (Wire, Vec<Observed>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let (result, probes) = if owned {
                let (mut application, probes) = H1App::new(vec![response]);
                application.respond_early = respond_early;
                let (session, _handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                (session.run().await, probes)
            } else {
                let handler = Rc::new(ScriptHandler {
                    respond_early,
                    ..ScriptHandler::new(vec![response])
                });
                let erased: HttpHandlerRef = handler.clone();
                let (session, _handle) = UniplexSession::new(
                    context,
                    TransportDirection::Downstream,
                    transport,
                    Some(erased),
                    h1_config(),
                )
                .unwrap();
                (session.run().await, Rc::clone(&handler.probes))
            };
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), observed)
        })
    })
}

/// Peer EOF read while an upgrade or CONNECT head waits for its final response, with no tunnel
/// bytes, is delivered once that response resolves the upgrade: an accepted tunnel's request
/// body ends and a rejected request ends, so the exchange retires and `run` returns, with the
/// same bytes, close and result as `UniplexSession`, however the input is cut.
#[test]
fn a_pending_upgrade_keeps_a_peer_eof() {
    let mut cases = Vec::new();
    for (head, websocket, refusal) in [(CONNECT_HEAD, false, 403), (WEBSOCKET_HEAD, true, 400)] {
        let name = String::from_utf8_lossy(&head[..7]).into_owned();
        let accepting = Response {
            websocket,
            ..Response::fixed(200, &[b'r'; 64])
        };
        cases.push((head, format!("{name} early fixed"), accepting, true));
        cases.push((
            head,
            format!("{name} early tunnel"),
            Response::tunnel(200, &[b'r'; 64], websocket),
            true,
        ));
        cases.push((
            head,
            format!("{name} body read first"),
            Response::tunnel(200, &[b'r'; 64], websocket),
            false,
        ));
        cases.push((
            head,
            format!("{name} refused"),
            Response {
                tunnel: true,
                ..Response::fixed(refusal, b"no")
            },
            false,
        ));
    }
    for (head, name, response, respond_early) in cases {
        let reads = |split: &str| match split {
            "whole" => vec![ReadAction::Data(head.to_vec()), ReadAction::Eof],
            "whole with EOF" => vec![ReadAction::DataEof(head.to_vec())],
            _ => one_byte_reads(head)
                .into_iter()
                .chain([ReadAction::Eof])
                .collect(),
        };
        for split in ["whole", "whole with EOF", "one byte"] {
            let label = format!("{name}, {split}");
            let (owned, owned_requests) =
                early_wire(true, reads(split), response.clone(), respond_early);
            let (uniplex, uniplex_requests) =
                early_wire(false, reads(split), response.clone(), respond_early);
            assert_eq!(owned_requests.len(), 1, "{label}");
            assert_eq!(owned_requests[0].response_complete, 1, "{label}");
            assert_eq!(
                owned_requests[0].completed, 1,
                "{label}: the exchange retires once"
            );
            assert_same_requests(&owned_requests, &uniplex_requests, &label);
            assert_eq!(
                String::from_utf8_lossy(&owned.output),
                String::from_utf8_lossy(&uniplex.output),
                "{label}"
            );
            assert_eq!(owned.close, uniplex.close, "{label}");
            assert_eq!(owned.error, uniplex.error, "{label}");
        }
    }
}

/// Writes and closes through a [`ScriptedTransport`]. Each read observes its cancellation only
/// when polled, and does not arrange a wake for it.
struct PolledCancellationTransport(Rc<ScriptedTransport>);

impl UniplexTransport for PolledCancellationTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        Box::pin(std::future::poll_fn(move |_| {
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"read cancelled",
                )))
            } else {
                Poll::Pending
            }
        }))
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.0.write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.0.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.0.register_byte_event(context, registration)
    }
}

/// The idle close polls the outstanding read again after requesting its cancellation, so a read
/// that observes cancellation only when polled still completes the close.
#[test]
fn idle_close_polls_the_read_again_after_cancelling_it() {
    run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let transport = Rc::new(PolledCancellationTransport(inner));
            let (application, probes) = H1App::new(Vec::new());
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.initiate_drain().unwrap());
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

/// An idle close parks while its outstanding read has yet to complete, and finishes once that
/// read completes.
#[test]
fn idle_close_parks_until_the_read_completes() {
    run_on_host_with(|host, context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // The read ignores cancellation: it completes only when the test releases it.
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitData(Arc::clone(&gate), Vec::new())]);
            let (application, probes) = H1App::new(Vec::new());
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.initiate_drain().unwrap());
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                let yields = host.yields();
                for _ in 0..1000 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    host.yields(),
                    yields,
                    "the idle close kept polling while its read was outstanding"
                );
                assert_eq!(state.borrow().close, CloseKind::Open);
                gate.notify_one();
            });
            run.unwrap();
            assert!(probes.borrow().is_empty());
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

/// A chunked POST whose upload is still open.
const OPEN_UPLOAD: &[u8] =
    b"POST /u HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n";
/// The rest of `OPEN_UPLOAD`'s body.
const UPLOAD_END: &[u8] = b"5\r\nfinal\r\n0\r\n\r\n";
const NEXT_GET: &[u8] = b"GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";
/// The end of `trailered_response`'s bytes.
const TRAILERED_END: &str = "\r\n\r\n5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\n";
/// How many times a `stop_run` script yields to let the connection finish before it offers
/// more input.
const STOP_SETTLE: usize = 256;

/// A chunked response whose trailer section ends it.
fn trailered_response() -> Response {
    Response {
        content_length: false,
        trailers: vec![("x-trailer", "t")],
        ..Response::fixed(200, b"reply")
    }
}

/// One `stop_run`.
struct StopCase {
    stop: (HttpErrorCode, StopAt),
    first: Vec<u8>,
    /// Offered once the connection is idle, if it still reads. Peer EOF follows it.
    rest: Vec<u8>,
    response: Response,
    /// A write that carries these bytes completes only once `rest` was offered and the
    /// connection went idle.
    hold: Option<&'static [u8]>,
}

struct StopRun {
    wire: Wire,
    /// What the transport had written when `StopAt::Body` or `StopAt::Last` released the stop,
    /// or when `StopAt::Script` stopped the request; empty for `StopAt::Head` and
    /// `StopAt::Refusal`.
    before_stop: String,
    observed: Vec<Observed>,
}

/// Yields until `done` is set, at most `STOP_SETTLE` times.
async fn settle_unless_done(context: &WorkerContext, done: &Cell<bool>) {
    for _ in 0..STOP_SETTLE {
        if done.get() {
            return;
        }
        context.host().yield_to_io().await;
    }
}

/// Serves `case` with the owned driver. Each exchange with a request body stops it with
/// `case.stop`, and `StopAt::Body` or `StopAt::Last` releases the stop once the transport has
/// written something. `StopAt::Script` releases the response, lets the connection go idle, and
/// then stops the request.
fn stop_run(case: StopCase) -> StopRun {
    run_on_host(move |context| {
        Box::pin(async move {
            let offer = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(case.first),
                ReadAction::WaitDataOrCancellation(Arc::clone(&offer), case.rest),
                ReadAction::Eof,
            ]);
            let release = Rc::new(tokio::sync::Notify::new());
            if let Some(held) = case.hold {
                state.borrow_mut().write_gate = Some((held.to_vec(), Rc::clone(&release)));
            }
            let gate = Rc::new(tokio::sync::Notify::new());
            let (mut application, probes) = H1App::new(vec![case.response]);
            application.stop_request = Some(case.stop);
            application.body_gate = Some(Rc::clone(&gate));
            let script_held = Rc::clone(&application.script_held);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let done = Cell::new(false);
            let run = async {
                let result = session.run().await;
                done.set(true);
                result
            };
            let script = async {
                let mut before_stop = Vec::new();
                if matches!(case.stop.1, StopAt::Body | StopAt::Last) {
                    before_stop = first_writes(&context, &state).await;
                    gate.notify_one();
                }
                if matches!(case.stop.1, StopAt::Script) {
                    gate.notify_one();
                    settle_unless_done(&context, &done).await;
                    before_stop = state.borrow().writes.concat();
                    let held = script_held.borrow_mut().take();
                    if let Some(mut body) = held {
                        body.stop_reading(Some(case.stop.0));
                    }
                }
                if let Some(held) = case.hold {
                    for _ in 0..STOP_SETTLE {
                        let written = state.borrow().writes.concat();
                        if done.get() || written.windows(held.len()).any(|part| part == held) {
                            break;
                        }
                        context.host().yield_to_io().await;
                    }
                    offer.notify_one();
                    settle_unless_done(&context, &done).await;
                    release.notify_one();
                } else {
                    settle_unless_done(&context, &done).await;
                    offer.notify_one();
                }
                before_stop
            };
            let (result, before_stop) = futures::join!(run, script);
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            StopRun {
                wire: wire_of(&state, result),
                before_stop: String::from_utf8_lossy(&before_stop).into_owned(),
                observed,
            }
        })
    })
}

/// The exchange got its whole response and completed once, uncancelled.
fn assert_completed_once(probe: &Observed, label: &str) {
    assert_eq!(probe.response_complete, 1, "{label}");
    assert_eq!(probe.completed, 1, "{label}");
    assert!(probe.cancelled.is_empty(), "{label}: {:?}", probe.cancelled);
}

/// A request whose reader stops it without error (`NO_ERROR`) before its response begins,
/// with its upload still open, gets its whole response, which carries `Connection: close` and
/// ends with its trailer section. Then the connection closes, and the rest of the upload is
/// not read as a request.
#[test]
fn a_request_stopped_without_error_gets_its_response_with_connection_close() {
    let run = stop_run(StopCase {
        stop: (HttpErrorCode::NO_ERROR, StopAt::Head),
        first: OPEN_UPLOAD.to_vec(),
        rest: [UPLOAD_END, NEXT_GET].concat(),
        response: trailered_response(),
        hold: None,
    });
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(
        output.ends_with(TRAILERED_END),
        "the whole response, trailer section included: {output:?}"
    );
    assert_eq!(
        run.wire.close,
        CloseKind::ShutdownWrite,
        "then the transport closes"
    );
    assert_eq!(run.observed.len(), 1, "no next request is read");
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A request whose reader stops it with `CANCEL` before its response ends still fails the
/// connection. The session stops writing, and the transport resets.
#[test]
fn a_request_stopped_with_cancel_still_fails_the_connection() {
    for at in [StopAt::Head, StopAt::Body] {
        let label = format!("{at:?}");
        let run = stop_run(StopCase {
            stop: (HttpErrorCode::CANCEL, at),
            first: OPEN_UPLOAD.to_vec(),
            rest: [UPLOAD_END, NEXT_GET].concat(),
            response: trailered_response(),
            hold: None,
        });
        assert_eq!(run.wire.error, Some(HttpErrorCode::CANCEL), "{label}");
        assert_eq!(run.wire.close, CloseKind::Reset, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&run.wire.output),
            run.before_stop,
            "{label}: nothing is written after the stop"
        );
        assert_eq!(run.observed.len(), 1, "{label}");
        assert_eq!(run.observed[0].completed, 0, "{label}");
        assert_eq!(
            run.observed[0].cancelled,
            [(HttpErrorCode::CANCEL, false)],
            "{label}"
        );
    }
}

/// A request read to its end, then responded to, leaves the connection serving the next
/// request.
#[test]
fn a_request_read_to_its_end_keeps_the_connection_alive() {
    let input = [
        OPEN_UPLOAD,
        UPLOAD_END,
        b"GET /2 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
    ]
    .concat();
    let (owned, probes) = owned_wire(vec![ReadAction::Data(input)], vec![trailered_response()]);
    assert_eq!(owned.error, None);
    let urls: Vec<_> = probes.iter().map(|probe| probe.url.clone()).collect();
    assert_eq!(urls, [b"/u".to_vec(), b"/2".to_vec()]);
    assert_eq!(probes[0].request_body, b"firstfinal");
    for probe in &probes {
        assert_completed_once(probe, "each request");
    }
    let head =
        "HTTP/1.1 200 \r\nDate: Tue, 03 Nov 2026 00:00:00 GMT\r\nTransfer-Encoding: chunked\r\n";
    let body = "\r\n5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\n";
    assert_eq!(
        String::from_utf8_lossy(&owned.output),
        format!("{head}Connection: keep-alive\r\n{body}{head}Connection: close\r\n{body}"),
        "the first response keeps the connection, the second closes it"
    );
    assert_eq!(owned.close, CloseKind::ShutdownWrite);
}

/// A CONNECT tunnel whose reader stops its request body without error before the tunnel's
/// response ends still fails the connection. The session stops writing, and the transport
/// resets.
#[test]
fn a_tunnel_stopped_without_error_still_fails_the_connection() {
    for at in [StopAt::Head, StopAt::Body] {
        let label = format!("{at:?}");
        let run = stop_run(StopCase {
            stop: (HttpErrorCode::NO_ERROR, at),
            first: [CONNECT_HEAD, b"up"].concat(),
            rest: b"more".to_vec(),
            response: Response::tunnel(200, b"down", false),
            hold: None,
        });
        if matches!(at, StopAt::Body) {
            assert!(
                run.before_stop.starts_with("HTTP/1.1 200 "),
                "the tunnel was accepted: {:?}",
                run.before_stop
            );
        }
        assert_eq!(run.wire.error, Some(HttpErrorCode::NO_ERROR), "{label}");
        assert_eq!(run.wire.close, CloseKind::Reset, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&run.wire.output),
            run.before_stop,
            "{label}: nothing is written after the stop"
        );
        assert_eq!(run.observed.len(), 1, "{label}");
        assert_eq!(run.observed[0].completed, 0, "{label}");
        assert_eq!(
            run.observed[0].cancelled,
            [(HttpErrorCode::NO_ERROR, false)],
            "{label}"
        );
    }
}

/// A request whose reader stops it without error after its response head went out with
/// `Connection: keep-alive`, while its upload is still open, gets the rest of its response,
/// chunked with its trailer section or to its fixed length. Then the connection closes, and
/// the rest of the upload is not read as a request.
#[test]
fn a_request_stopped_after_a_keep_alive_head_closes_after_its_response() {
    let fixed = Response::fixed(200, b"reply");
    for (response, end) in [
        (
            trailered_response(),
            "5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\n",
        ),
        (fixed, "reply"),
    ] {
        let label = format!("content_length {}", response.content_length);
        let run = stop_run(StopCase {
            stop: (HttpErrorCode::NO_ERROR, StopAt::Body),
            first: OPEN_UPLOAD.to_vec(),
            rest: [UPLOAD_END, NEXT_GET].concat(),
            response,
            hold: None,
        });
        assert!(
            run.before_stop.contains("\r\nConnection: keep-alive\r\n")
                && run.before_stop.ends_with("\r\n\r\n"),
            "{label}: the head alone was written before the stop: {:?}",
            run.before_stop
        );
        assert_eq!(run.wire.error, None, "{label}");
        assert_eq!(
            String::from_utf8_lossy(&run.wire.output),
            format!("{}{end}", run.before_stop),
            "{label}: the rest of the response follows the head"
        );
        assert_eq!(run.wire.close, CloseKind::ShutdownWrite, "{label}");
        assert_eq!(run.observed.len(), 1, "{label}: no next request is read");
        assert_completed_once(&run.observed[0], &label);
    }
}

/// A request whose reader stops it without error in the same poll that returns its response's
/// last event, after a `Connection: keep-alive` head and with its upload still open, gets the
/// rest of its response. Then the connection closes, and the rest of the upload is not read as
/// a request.
#[test]
fn a_request_stopped_with_its_last_response_event_closes_after_its_response() {
    for (response, end) in [
        (
            trailered_response(),
            "5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\n",
        ),
        (Response::fixed(200, b"reply"), "\r\n\r\nreply"),
    ] {
        let label = format!("content_length {}", response.content_length);
        let run = stop_run(StopCase {
            stop: (HttpErrorCode::NO_ERROR, StopAt::Last),
            first: OPEN_UPLOAD.to_vec(),
            rest: [UPLOAD_END, NEXT_GET].concat(),
            response,
            hold: None,
        });
        assert!(
            run.before_stop.contains("\r\nConnection: keep-alive\r\n"),
            "{label}: the head was written before the stop: {:?}",
            run.before_stop
        );
        let output = String::from_utf8_lossy(&run.wire.output);
        assert_eq!(run.wire.error, None, "{label}: {output:?}");
        assert_eq!(
            output.matches("HTTP/1.1 200 ").count(),
            1,
            "{label}: {output:?}"
        );
        assert!(
            output.ends_with(end),
            "{label}: the whole response: {output:?}"
        );
        assert_eq!(run.wire.close, CloseKind::ShutdownWrite, "{label}");
        assert_eq!(run.observed.len(), 1, "{label}: no next request is read");
        assert_completed_once(&run.observed[0], &label);
    }
}

/// A request whose reader stops it without error from the cancellation callback of a byte
/// event on its response's last event, which the driver cancels before it serializes that
/// event, after a `Connection: keep-alive` head and with its upload still open, gets the rest
/// of its response. Then the connection closes, and the rest of the upload is not read as a
/// request.
#[test]
fn a_request_stopped_by_its_last_response_events_byte_event_refusal_closes_after_it() {
    for (response, end) in [
        (
            trailered_response(),
            "5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\n",
        ),
        (Response::fixed(200, b"reply"), "\r\n\r\nreply"),
    ] {
        let label = format!("content_length {}", response.content_length);
        let run = stop_run(StopCase {
            stop: (HttpErrorCode::NO_ERROR, StopAt::Refusal),
            first: OPEN_UPLOAD.to_vec(),
            rest: [UPLOAD_END, NEXT_GET].concat(),
            response,
            hold: None,
        });
        let output = String::from_utf8_lossy(&run.wire.output);
        assert_eq!(run.wire.error, None, "{label}: {output:?}");
        assert_eq!(
            output.matches("HTTP/1.1 200 ").count(),
            1,
            "{label}: {output:?}"
        );
        assert!(
            output.contains("\r\nConnection: keep-alive\r\n"),
            "{label}: the head was serialized before the stop: {output:?}"
        );
        assert!(
            output.ends_with(end),
            "{label}: the whole response: {output:?}"
        );
        assert_eq!(run.wire.close, CloseKind::ShutdownWrite, "{label}");
        assert_eq!(run.observed.len(), 1, "{label}: no next request is read");
        assert_completed_once(&run.observed[0], &label);
    }
}

/// After its reader stops a request without error, the rest of its upload is not parsed. Input
/// and a peer EOF inside the request, read while the response is still being written, do not
/// end the exchange, which gets its whole response before the connection closes.
#[test]
fn a_stopped_upload_is_not_parsed_while_its_response_is_written() {
    let run = stop_run(StopCase {
        stop: (HttpErrorCode::NO_ERROR, StopAt::Head),
        first: OPEN_UPLOAD.to_vec(),
        rest: b"5\r\nmore!\r\n".to_vec(),
        response: trailered_response(),
        hold: Some(b"x-trailer"),
    });
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert_eq!(run.observed.len(), 1);
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A request that arrived whole before its reader stopped it without error does not leave
/// upload bytes unread on the connection. It gets its whole response with
/// `Connection: keep-alive`, and the connection serves the next request.
#[test]
fn a_whole_request_stopped_without_error_keeps_the_connection_alive() {
    let run = stop_run(StopCase {
        stop: (HttpErrorCode::NO_ERROR, StopAt::Head),
        first: [OPEN_UPLOAD, UPLOAD_END].concat(),
        rest: NEXT_GET.to_vec(),
        response: trailered_response(),
        hold: None,
    });
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert!(
        output.starts_with(
            "HTTP/1.1 200 \r\nDate: Tue, 03 Nov 2026 00:00:00 GMT\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nreply\r\n0\r\nx-trailer: t\r\n\r\nHTTP/1.1 200 "
        ),
        "the whole first response keeps the connection: {output:?}"
    );
    let urls: Vec<_> = run.observed.iter().map(|probe| probe.url.clone()).collect();
    assert_eq!(urls, [b"/u".to_vec(), b"/2".to_vec()]);
    for probe in &run.observed {
        assert_completed_once(probe, "each request");
    }
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
}

/// A request whose reader stops it without error only once its whole response was written,
/// with its upload still open, keeps the connection. The rest of the upload is read and
/// discarded, and the next request is served.
#[test]
fn a_request_stopped_after_its_response_ended_keeps_the_connection() {
    let run = stop_run(StopCase {
        stop: (HttpErrorCode::NO_ERROR, StopAt::Script),
        first: OPEN_UPLOAD.to_vec(),
        rest: [UPLOAD_END, NEXT_GET].concat(),
        response: trailered_response(),
        hold: None,
    });
    assert!(
        run.before_stop.contains("\r\nConnection: keep-alive\r\n")
            && run.before_stop.ends_with(TRAILERED_END),
        "the whole response was written before the stop: {:?}",
        run.before_stop
    );
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    let urls: Vec<_> = run.observed.iter().map(|probe| probe.url.clone()).collect();
    assert_eq!(urls, [b"/u".to_vec(), b"/2".to_vec()], "{output:?}");
    for probe in &run.observed {
        assert_completed_once(probe, "each request");
    }
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 2, "{output:?}");
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
}

/// One `staged_close_run`.
struct StagedClose {
    wire: Wire,
    /// Whether the run was still going when the transport's write side shut down.
    reading_after_shutdown: bool,
    /// Scripted reads the driver never took.
    unread: usize,
    /// With `fill`, the scripted reads the driver had not taken when the held write was
    /// released.
    unread_at_release: Option<usize>,
    /// With `offer`, the scripted reads the driver had not taken when the script stopped the
    /// request.
    unread_at_stop: Option<usize>,
    observed: Vec<Observed>,
}

/// Serves `reads` with the owned driver, configured by `configure`. Each exchange with a
/// request body stops it without error in `poll_headers`. With `fill`, the write carrying the
/// response's trailer section is held: `fill` is notified once that write starts, and the
/// write completes once the connection is idle. With `offer`, the stop comes from outside the
/// driver's poll instead. Once the exchange holds its request body, the script stops it without
/// error, notifies `offer` and releases the response in one step, so the driver's next poll
/// observes both the stop and any read that `offer` completes. `go` is notified once the
/// transport's write side shuts down, or once the run ends.
fn staged_close_run(
    reads: Vec<ReadAction>,
    fill: Option<Arc<tokio::sync::Notify>>,
    offer: Option<Arc<tokio::sync::Notify>>,
    go: Arc<tokio::sync::Notify>,
    configure: impl FnOnce(&mut UniplexConfig) + Send + 'static,
) -> StagedClose {
    const HELD: &[u8] = b"x-trailer";
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(reads);
            let release = Rc::new(tokio::sync::Notify::new());
            if fill.is_some() {
                state.borrow_mut().write_gate = Some((HELD.to_vec(), Rc::clone(&release)));
            }
            let (mut application, probes) = H1App::new(vec![trailered_response()]);
            let at = if offer.is_some() {
                StopAt::Script
            } else {
                StopAt::Head
            };
            application.stop_request = Some((HttpErrorCode::NO_ERROR, at));
            let gate = Rc::new(tokio::sync::Notify::new());
            if offer.is_some() {
                application.body_gate = Some(Rc::clone(&gate));
            }
            let script_held = Rc::clone(&application.script_held);
            let mut options = owned_options(context.clone(), transport);
            configure(&mut options.io);
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let done = Cell::new(false);
            let run = async {
                let result = session.run().await;
                done.set(true);
                result
            };
            let script = async {
                let mut unread_at_release = None;
                let mut unread_at_stop = None;
                if let Some(offer) = &offer {
                    for _ in 0..STOP_SETTLE {
                        if done.get() || script_held.borrow().is_some() {
                            break;
                        }
                        context.host().yield_to_io().await;
                    }
                    unread_at_stop = Some(state.borrow().reads.len());
                    let held = script_held.borrow_mut().take();
                    if let Some(mut body) = held {
                        body.stop_reading(Some(HttpErrorCode::NO_ERROR));
                    }
                    offer.notify_one();
                    gate.notify_one();
                }
                if let Some(fill) = &fill {
                    for _ in 0..STOP_SETTLE {
                        let written = state.borrow().writes.concat();
                        if done.get() || written.windows(HELD.len()).any(|part| part == HELD) {
                            break;
                        }
                        context.host().yield_to_io().await;
                    }
                    fill.notify_one();
                    settle_unless_done(&context, &done).await;
                    unread_at_release = Some(state.borrow().reads.len());
                    release.notify_one();
                }
                for _ in 0..STOP_SETTLE {
                    if done.get() || state.borrow().close != CloseKind::Open {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                let reading = !done.get() && state.borrow().close == CloseKind::ShutdownWrite;
                go.notify_one();
                (reading, unread_at_release, unread_at_stop)
            };
            let (result, (reading_after_shutdown, unread_at_release, unread_at_stop)) =
                futures::join!(run, script);
            let observed = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            let unread = state.borrow().reads.len();
            StagedClose {
                wire: wire_of(&state, result),
                reading_after_shutdown,
                unread,
                unread_at_release,
                unread_at_stop,
                observed,
            }
        })
    })
}

/// The RFC 9112 staged close after a request's reader stops it without error while its upload
/// is still open. Once the response is written, the write side shuts down first, and the
/// connection keeps reading the rest of the upload, without parsing it, until the peer's EOF,
/// so that it does not close with input unread.
#[test]
fn a_stopped_upload_is_read_to_peer_eof_after_the_write_side_shuts_down() {
    let go = Arc::new(tokio::sync::Notify::new());
    let run = staged_close_run(
        vec![
            ReadAction::Data(OPEN_UPLOAD.to_vec()),
            ReadAction::WaitDataOrCancellation(Arc::clone(&go), [UPLOAD_END, NEXT_GET].concat()),
            ReadAction::Eof,
        ],
        None,
        None,
        go,
        |_| {},
    );
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert!(
        run.reading_after_shutdown,
        "the write side shuts down while the connection still reads"
    );
    assert_eq!(
        run.unread, 0,
        "the rest of the upload and the peer's EOF are read before the run ends"
    );
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(
        run.observed.len(),
        1,
        "nothing read after the stop is parsed as a request"
    );
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// Under `abort_exchanges_on_eof`, the staged close after a stopped upload's response behaves
/// the same. The peer's EOF read during it does not abort, and the run ends at that EOF without
/// an error.
#[test]
fn staged_close_under_eof_abort_reads_to_peer_eof() {
    let go = Arc::new(tokio::sync::Notify::new());
    let run = staged_close_run(
        vec![
            ReadAction::Data(OPEN_UPLOAD.to_vec()),
            ReadAction::WaitDataOrCancellation(Arc::clone(&go), [UPLOAD_END, NEXT_GET].concat()),
            ReadAction::Eof,
        ],
        None,
        None,
        go,
        |io| io.abort_exchanges_on_eof = true,
    );
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert!(run.reading_after_shutdown, "{output:?}");
    assert_eq!(run.unread, 0, "the peer's EOF is read before the run ends");
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A staged close after a stopped upload reads at most `buffer_limit` bytes after its write
/// side shuts down, then closes with the rest of the upload unread.
#[test]
fn a_staged_close_reads_a_stopped_upload_only_up_to_the_buffer_limit() {
    const LIMIT: usize = 4 << 10;
    const READ: usize = 1 << 10;
    let go = Arc::new(tokio::sync::Notify::new());
    let mut reads = vec![
        ReadAction::Data(OPEN_UPLOAD.to_vec()),
        ReadAction::WaitDataOrCancellation(Arc::clone(&go), vec![b'u'; READ]),
    ];
    reads.extend((0..LIMIT / READ).map(|_| ReadAction::Data(vec![b'u'; READ])));
    reads.push(ReadAction::Eof);
    let run = staged_close_run(reads, None, None, go, |io| {
        io.buffer_limit = LIMIT;
        io.ingress_buffer_limit = LIMIT as u32;
        io.maximum_read_size = READ;
        io.read_allocation_size = READ;
        io.maximum_write_size = LIMIT;
    });
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert!(
        run.reading_after_shutdown,
        "the write side shuts down while the connection still reads"
    );
    assert_eq!(
        run.unread, 2,
        "{LIMIT} bytes are read after the write side shuts down; one more read and the \
         peer's EOF are not"
    );
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(run.observed.len(), 1);
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A staged close after a stopped upload still reads after the write side shuts down when the
/// upload filled the input buffer, `buffer_limit`, while the response was still being written:
/// the connection reads on, without parsing, until the peer's EOF.
#[test]
fn a_staged_close_reads_after_the_write_side_shuts_down_when_the_upload_filled_its_buffer() {
    const LIMIT: usize = 4 << 10;
    const READ: usize = 1 << 10;
    let fill = Arc::new(tokio::sync::Notify::new());
    let go = Arc::new(tokio::sync::Notify::new());
    let mut reads = vec![
        ReadAction::Data(OPEN_UPLOAD.to_vec()),
        ReadAction::WaitDataOrCancellation(Arc::clone(&fill), vec![b'u'; READ]),
    ];
    reads.extend((1..LIMIT / READ).map(|_| ReadAction::Data(vec![b'u'; READ])));
    reads.push(ReadAction::WaitDataOrCancellation(
        Arc::clone(&go),
        vec![b'u'; READ],
    ));
    reads.push(ReadAction::Eof);
    let run = staged_close_run(reads, Some(fill), None, go, |io| {
        io.buffer_limit = LIMIT;
        io.ingress_buffer_limit = LIMIT as u32;
        io.maximum_read_size = READ;
        io.read_allocation_size = READ;
        io.maximum_write_size = LIMIT;
    });
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert_eq!(
        run.unread_at_release,
        Some(2),
        "{LIMIT} bytes of the upload filled the input buffer before the response's last write \
         completed"
    );
    assert!(
        run.reading_after_shutdown,
        "the write side shuts down while the connection still reads"
    );
    assert_eq!(
        run.unread, 0,
        "the rest of the upload and the peer's EOF are read after the write side shuts down"
    );
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(
        run.observed.len(),
        1,
        "nothing read after the stop is parsed as a request"
    );
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A stop reported outside the driver's poll takes effect before the input that poll reads is
/// parsed. Peer EOF inside the stopped request, read in the same poll that observes the stop,
/// does not end the exchange, which gets its whole response before the connection closes.
#[test]
fn a_stop_found_with_peer_eof_in_the_same_poll_still_gets_its_response() {
    let offer = Arc::new(tokio::sync::Notify::new());
    let run = staged_close_run(
        vec![
            ReadAction::Data(OPEN_UPLOAD.to_vec()),
            ReadAction::WaitDataOrCancellation(Arc::clone(&offer), b"5\r\nmore!\r\n".to_vec()),
            ReadAction::Eof,
        ],
        None,
        Some(offer),
        Arc::new(tokio::sync::Notify::new()),
        |_| {},
    );
    assert_eq!(
        run.unread_at_stop,
        Some(1),
        "the read the stop's step completes was outstanding at the stop, and the EOF behind it \
         untaken"
    );
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert_eq!(run.unread, 0, "the peer's EOF was read");
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
    assert_eq!(run.observed.len(), 1);
    assert_completed_once(&run.observed[0], "the stopped request");
}

/// A stop reported outside the driver's poll takes effect before the input that poll reads is
/// parsed. The rest of the stopped upload and a next request, read in the same poll that
/// observes the stop, are not parsed, so the request does not end and the connection is not
/// kept. The response carries `Connection: close`, and the staged close follows it.
#[test]
fn a_stop_found_with_the_rest_of_the_upload_in_the_same_poll_closes_after_the_response() {
    let offer = Arc::new(tokio::sync::Notify::new());
    let go = Arc::new(tokio::sync::Notify::new());
    let run = staged_close_run(
        vec![
            ReadAction::Data(OPEN_UPLOAD.to_vec()),
            ReadAction::WaitDataOrCancellation(Arc::clone(&offer), [UPLOAD_END, NEXT_GET].concat()),
            ReadAction::WaitEof(Arc::clone(&go)),
        ],
        None,
        Some(offer),
        go,
        |_| {},
    );
    assert_eq!(
        run.unread_at_stop,
        Some(1),
        "the read the stop's step completes was outstanding at the stop"
    );
    let output = String::from_utf8_lossy(&run.wire.output);
    assert_eq!(run.wire.error, None, "{output:?}");
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(
        run.observed.len(),
        1,
        "nothing read with the stop is parsed as a request"
    );
    assert_completed_once(&run.observed[0], "the stopped request");
    assert!(
        run.reading_after_shutdown,
        "the write side shuts down while the connection still reads"
    );
    assert_eq!(run.unread, 0, "the peer's EOF is read before the run ends");
    assert_eq!(run.wire.close, CloseKind::ShutdownWrite);
}

/// Records the write side's shutdown on a [`ScriptedTransport`], then reports it failed.
struct FailedShutdownTransport(Rc<ScriptedTransport>);

impl UniplexTransport for FailedShutdownTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.0
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.0.write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.shutdown_write(context)?;
        Err(HttpError::with_message(
            HttpErrorCode::TRANSPORT_WRITE_ERROR,
            b"scripted shutdown failure",
        ))
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.0.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.0.register_byte_event(context, registration)
    }
}

/// A staged close after a stopped upload whose write shutdown fails returns that error
/// immediately. It does not wait for the peer's EOF or for `buffer_limit` more bytes from a
/// peer that has stopped sending.
#[test]
fn a_staged_close_returns_a_failed_write_shutdown_without_waiting_for_the_peer() {
    let (wire, returned_before_cancel, observed) = run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([ReadAction::Data(OPEN_UPLOAD.to_vec())]);
            let transport = Rc::new(FailedShutdownTransport(inner));
            let (mut application, probes) = H1App::new(vec![trailered_response()]);
            application.stop_request = Some((HttpErrorCode::NO_ERROR, StopAt::Head));
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context.clone(), transport))
                    .unwrap();
            let done = Cell::new(false);
            let run = async {
                let result = session.run().await;
                done.set(true);
                result
            };
            let script = async {
                for _ in 0..STOP_SETTLE {
                    if done.get() || state.borrow().close != CloseKind::Open {
                        break;
                    }
                    context.host().yield_to_io().await;
                }
                settle_unless_done(&context, &done).await;
                let returned = done.get();
                if !returned {
                    handle.cancel();
                }
                returned
            };
            let (result, returned_before_cancel) = futures::join!(run, script);
            let observed: Vec<_> = probes
                .borrow()
                .iter()
                .map(|probe| probe.observed())
                .collect();
            (wire_of(&state, result), returned_before_cancel, observed)
        })
    });
    let output = String::from_utf8_lossy(&wire.output);
    assert!(
        returned_before_cancel,
        "the run returns once the write shutdown fails, with the peer idle: {:?}",
        wire.error
    );
    assert_eq!(
        wire.error,
        Some(HttpErrorCode::TRANSPORT_WRITE_ERROR),
        "{output:?}"
    );
    assert_eq!(wire.close, CloseKind::ShutdownWrite);
    assert_eq!(output.matches("HTTP/1.1 200 ").count(), 1, "{output:?}");
    assert!(output.contains("\r\nConnection: close\r\n"), "{output:?}");
    assert!(output.ends_with(TRAILERED_END), "{output:?}");
    assert_eq!(observed.len(), 1);
    assert_completed_once(&observed[0], "the stopped request");
}

/// A response body the test pushes one event at a time to a `PushedApp` exchange.
#[derive(Default)]
struct PushedBody {
    events: RefCell<VecDeque<HttpBodyEvent>>,
    waker: RefCell<Option<Waker>>,
    /// `poll_body` calls.
    polls: Cell<usize>,
    /// When set, the exchange hands its request body source to `request`, unread; otherwise it
    /// drops it.
    keep_request: bool,
    request: RefCell<Option<HttpSourceHolder>>,
    /// When set, a poll that returns an event wakes its waker first, as a source with more data
    /// ready behind the event may.
    wake_with_event: bool,
    /// Run once by the next poll that returns `Pending`, after it keeps its waker.
    on_pending: RefCell<Option<Box<dyn FnOnce()>>>,
    /// `response_complete` calls.
    response_completes: Cell<usize>,
}

impl PushedBody {
    fn push(&self, event: HttpBodyEvent) {
        self.events.borrow_mut().push_back(event);
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Responds to each request immediately with a chunked `200` head, then with the events pushed
/// to `body`.
struct PushedApp {
    body: Rc<PushedBody>,
}

struct PushedExchange {
    head: Option<HttpHeaderEvent>,
    body: Rc<PushedBody>,
}

impl OwnedApplication for PushedApp {
    type Exchange = PushedExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> PushedExchange {
        let mut message = HttpMessage::new();
        message.set_status_code(200).unwrap();
        message.set_http_version(1, 1);
        message.headers_mut().set(HttpHeaderCode::Date, DATE);
        PushedExchange {
            head: Some(HttpHeaderEvent::new(message, false).unwrap()),
            body: Rc::clone(&self.body),
        }
    }
}

impl OwnedExchange for PushedExchange {
    fn request(&mut self, _head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        if self.body.keep_request {
            *self.body.request.borrow_mut() = Some(body.into_source_holder());
        }
    }

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        Poll::Ready(Ok(self
            .head
            .take()
            .expect("the response head is polled once")))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.body.polls.set(self.body.polls.get() + 1);
        let event = self.body.events.borrow_mut().pop_front();
        if let Some(event) = event {
            if self.body.wake_with_event {
                context.waker().wake_by_ref();
            }
            return Poll::Ready(Ok(event));
        }
        *self.body.waker.borrow_mut() = Some(context.waker().clone());
        let on_pending = self.body.on_pending.borrow_mut().take();
        if let Some(on_pending) = on_pending {
            on_pending();
        }
        Poll::Pending
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {
        self.body
            .response_completes
            .set(self.body.response_completes.get() + 1);
    }

    fn complete(self) {}
}

/// Serves a keep-alive GET whose chunked response head is written immediately, then pushes each
/// of `chunks` to its body and polls the driver once. Returns the driver turns, codec parses,
/// phase checks and body polls each push took, and every write.
fn pushed_body_turns(
    chunks: &'static [&'static [u8]],
) -> (Vec<(usize, usize, usize, usize)>, Vec<Vec<u8>>) {
    pushed_body_turns_through(false, chunks)
}

/// As [`pushed_body_turns`], with every write completing when first polled, as a socket write
/// does when the socket accepts all of it. Writes go directly to a [`ScriptedTransport`], or,
/// when `queued`, through an open [`FenceQueue`], the write queue path of an owned TCP connection.
fn pushed_body_turns_through(
    queued: bool,
    chunks: &'static [&'static [u8]],
) -> (Vec<(usize, usize, usize, usize)>, Vec<Vec<u8>>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let reads = [ReadAction::Data(KEEP_ALIVE_GET.to_vec())];
            let (transport, state): (Rc<dyn UniplexTransport>, _) = if queued {
                let (transport, state, fence) = FencedTransport::new(reads, true);
                fence.open.set(true);
                (transport, state)
            } else {
                let (transport, state) = ScriptedTransport::new(reads);
                (transport, state)
            };
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the response waits for its body"
            );
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            let mut turns = Vec::new();
            for chunk in chunks {
                let before = (
                    driver_turns(),
                    codec_parses(),
                    phase_checks(),
                    body.polls.get(),
                );
                body.push(HttpBodyEvent::body(queue(chunk), false));
                assert!(run.as_mut().poll(&mut poll).is_pending());
                turns.push((
                    driver_turns() - before.0,
                    codec_parses() - before.1,
                    phase_checks() - before.2,
                    body.polls.get() - before.3,
                ));
            }
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            let writes = state.borrow().writes.clone();
            (turns, writes)
        })
    })
}

/// Each response body event pushed to a waiting exchange takes one driver turn. The turn
/// serializes the event and, since only the exchange's phase has work left, polls the body
/// again in place of the next turn and counts that poll as the next turn. The body is pending,
/// so the event's write starts, and the write's completion is taken where the next turn would
/// have taken it. The body is still polled twice per event: once for the event and once until
/// it is pending. A turn never runs only to poll it again or to take the write's completion,
/// directly or through a write queue, and the parser, paused behind the request, gets no input.
#[test]
fn a_pushed_response_body_event_takes_one_driver_turn() {
    for queued in [false, true] {
        let (counts, writes) = pushed_body_turns_through(queued, &[b"one", b"two", b"three"]);
        let turns: Vec<_> = counts
            .iter()
            .map(|&(turns, parses, _, polls)| (turns, parses, polls))
            .collect();
        assert_eq!(
            turns,
            [(1, 0, 2); 3],
            "queued {queued}: (driver turns, codec parses, body polls) per event"
        );
        assert!(
            writes[0].starts_with(b"HTTP/1.1 200 \r\n"),
            "queued {queued}: {:?}",
            String::from_utf8_lossy(&writes[0])
        );
        assert_eq!(
            writes[1..],
            [
                b"3\r\none\r\n".to_vec(),
                b"3\r\ntwo\r\n".to_vec(),
                b"5\r\nthree\r\n".to_vec(),
                b"3\r\nend\r\n0\r\n\r\n".to_vec(),
            ],
            "queued {queued}: each event is written alone, as it arrives"
        );
    }
}

/// The driver checks at most once per turn whether a phase before the write would act, and once
/// more after it starts a write. If such a phase is due, it holds the write back and stays due
/// in the next turn, so the turn ends without a second check. Otherwise the turn starts the
/// write. The next turn's first phases, up to the write's completion, then run immediately, and
/// one check determines whether the rest of that turn would act. So each pushed body event
/// checks twice in its one turn: the write check, and once after its write. The check before
/// the body is polled again is separate and leaves out the exchange.
#[test]
fn a_pushed_response_body_event_checks_the_phases_twice() {
    for queued in [false, true] {
        let (counts, _) = pushed_body_turns_through(queued, &[b"one", b"two", b"three"]);
        let checks: Vec<_> = counts
            .iter()
            .map(|&(turns, _, checks, _)| (turns, checks))
            .collect();
        assert_eq!(
            checks,
            [(1, 2); 3],
            "queued {queued}: (driver turns, phase checks) per event"
        );
    }
}

/// Pushes one body event to a `PushedApp` exchange whose head was written, while the fence holds
/// writes, and polls the driver; then opens the fence and polls again. Returns the driver turns
/// of each poll and every write.
fn fenced_pushed_body_turns(queued: bool) -> ((usize, usize), Vec<Vec<u8>>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())], queued);
            fence.open.set(true);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            fence.open.set(false);
            let before = driver_turns();
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let pushed = driver_turns() - before;
            assert_eq!(state.borrow().writes.len(), 2, "the event's write started");
            let before = driver_turns();
            fence.open();
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let completed = driver_turns() - before;
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            let writes = state.borrow().writes.clone();
            ((pushed, completed), writes)
        })
    })
}

/// A pushed body event whose write does not complete when first polled takes one driver turn
/// as well. The turn serializes the event, polls the body again until it is pending, and starts
/// the write. The write's completion is still pending where the next turn would have taken it,
/// so the poll ends. The poll that the write's completion wakes takes it in one turn.
#[test]
fn a_pushed_body_event_whose_write_waits_takes_one_driver_turn() {
    for queued in [false, true] {
        let (turns, writes) = fenced_pushed_body_turns(queued);
        assert_eq!(
            turns,
            (1, 1),
            "queued {queued}: driver turns of the push's poll and of the completion's poll"
        );
        assert_eq!(
            writes[1..],
            [b"3\r\none\r\n".to_vec(), b"3\r\nend\r\n0\r\n\r\n".to_vec()],
            "queued {queued}"
        );
    }
}

/// A cancel that lands right after a body event is serialized, where another thread's worker
/// cancellation can land, is observed before the body is polled again, as when it landed before
/// the next turn: the exchange gets no further body poll, and the serialized event and the one
/// queued behind it are not written.
#[test]
fn a_cancel_landing_after_a_body_event_is_serialized_gets_no_further_body_poll() {
    let (polls, wire) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            body.push(HttpBodyEvent::body(queue(b"two"), false));
            let at_cancel = Rc::new(Cell::new(None));
            let (polls_at_cancel, polled) = (Rc::clone(&at_cancel), Rc::clone(&body));
            after_next_body_event_serialized(move || {
                polls_at_cancel.set(Some(polled.polls.get()));
                handle.cancel();
            });
            let result = run.await;
            let at_cancel = at_cancel.get().expect("a body event was serialized");
            (body.polls.get() - at_cancel, wire_of(&state, result))
        })
    });
    assert_eq!(polls, 0, "no body poll follows the cancel");
    assert_eq!(wire.error, Some(HttpErrorCode::CORO_CANCELLED));
    assert_eq!(wire.close, CloseKind::Reset);
    assert_eq!(wire.writes.len(), 1, "only the head is written");
}

/// A read that completes right after a body event is serialized, here the peer's EOF under
/// `abort_exchanges_on_eof`, is handled before the body is polled again, as when it completed
/// before the next turn. The exchange is still open, so the EOF aborts it and the event queued
/// behind the serialized one is never serialized. The serialized one is written, then the
/// connection resets.
#[test]
fn a_read_landing_after_a_body_event_is_serialized_is_handled_before_the_body_is_polled_again() {
    let wire = run_on_host(|context| {
        Box::pin(async move {
            let (scripted, state) = ScriptedTransport::new([
                ReadAction::Data(KEEP_ALIVE_GET.to_vec()),
                ReadAction::Eof,
            ]);
            let (transport, gate) = GatedReaderTransport::new(scripted);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let mut options = owned_options(context, transport);
            options.io.abort_exchanges_on_eof = true;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            gate.open_one();
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert!(
                gate.waiting(),
                "the request's read drained the transport and parked the reader"
            );
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            body.push(HttpBodyEvent::body(queue(b"two"), false));
            let eof = Rc::clone(&gate);
            after_next_body_event_serialized(move || eof.open_one());
            let result = run.await;
            wire_of(&state, result)
        })
    });
    assert_eq!(wire.error, Some(HttpErrorCode::TRANSPORT_EOF));
    assert_eq!(wire.close, CloseKind::Reset);
    assert!(
        wire.output.ends_with(b"\r\n\r\n3\r\none\r\n"),
        "only the serialized event follows the head: {:?}",
        String::from_utf8_lossy(&wire.output)
    );
}

/// A request body read that lands right after a response body event is serialized, and takes
/// the unread body below `ingress_buffer_limit`, lifts the flow-control pause before the body is
/// polled again, as when it lands before the next turn. An exchange that reads its request body
/// while it streams its response makes such reads. Parsing resumes in that poll, and the
/// request's end, which waited behind the pause, reaches the exchange.
#[test]
fn a_request_body_read_landing_after_a_body_event_is_serialized_resumes_parsing_in_that_poll() {
    let (read, parses, end) = run_on_host(|context| {
        Box::pin(async move {
            let upload = patterned(4 << 10);
            let mut request = format!(
                "POST /u HTTP/1.1\r\nHost: a\r\nContent-Length: {}\r\n\r\n",
                upload.len()
            )
            .into_bytes();
            request.extend_from_slice(&upload);
            let (transport, _state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let body = Rc::new(PushedBody {
                keep_request: true,
                ..PushedBody::default()
            });
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let mut options = owned_options(context, transport);
            options.io.ingress_buffer_limit = 1 << 10;
            let (session, handle) = OwnedH1Session::new(application, options).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let request = Rc::new(RefCell::new(
                body.request
                    .borrow_mut()
                    .take()
                    .expect("the exchange keeps its request body"),
            ));
            let read = Rc::new(Cell::new(0));
            let (reader, bytes) = (Rc::clone(&request), Rc::clone(&read));
            after_next_body_event_serialized(move || {
                let mut next = reader.borrow_mut().read_body_event(u32::MAX);
                let Poll::Ready(event) =
                    next.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("the paused request body is buffered");
                };
                let event = event.expect("the request body reads");
                bytes.set(event.as_body().map_or(0, |body| body.chain_length()));
            });
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            let before = codec_parses();
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let parses = codec_parses() - before;
            let end = {
                let mut next = request.borrow_mut().read_body_event(u32::MAX);
                match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(Ok(event)) => event.eom(),
                    Poll::Ready(Err(error)) => panic!("the request body fails: {error:?}"),
                    Poll::Pending => false,
                }
            };
            drop(request);
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            (read.get(), parses, end)
        })
    });
    assert!(
        read >= 1 << 10,
        "the read takes the unread body below the limit: {read} bytes"
    );
    assert_eq!(
        parses, 1,
        "parsing resumes in the poll that serialized the event"
    );
    assert!(end, "the request's end reaches the exchange in that poll");
}

/// A body whose poll wakes its own waker as it returns each event, as a source with more data
/// ready behind the event may, is still polled twice per event. The poll that follows the event
/// consumes that wake and gets `Pending` from the body, so the event is written alone in one
/// turn. The wake does not hold the write back for a further turn and poll.
#[test]
fn a_body_that_wakes_its_waker_with_each_event_is_polled_twice_per_event() {
    let (counts, writes) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(PushedBody {
                wake_with_event: true,
                ..PushedBody::default()
            });
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let counts: Vec<_> = [b"one".as_slice(), b"two", b"three"]
                .into_iter()
                .map(|chunk| {
                    let before = (driver_turns(), body.polls.get());
                    body.push(HttpBodyEvent::body(queue(chunk), false));
                    assert!(run.as_mut().poll(&mut poll).is_pending());
                    (driver_turns() - before.0, body.polls.get() - before.1)
                })
                .collect();
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            let writes = state.borrow().writes.clone();
            (counts, writes)
        })
    });
    assert_eq!(
        counts,
        [(1, 2); 3],
        "(driver turns, body polls) per pushed body event"
    );
    assert_eq!(
        writes[1..],
        [
            b"3\r\none\r\n".to_vec(),
            b"3\r\ntwo\r\n".to_vec(),
            b"5\r\nthree\r\n".to_vec(),
            b"3\r\nend\r\n0\r\n\r\n".to_vec(),
        ]
    );
}

/// The write decision that follows each serialized body event checks the write queue's capacity
/// even though the write waits behind the exchange, and that check may cancel from inside the
/// transport. The cancel is observed before the body is polled again, as when that decision
/// ended its turn: the exchange gets no further body poll, so the response's last event, queued
/// behind the first, is neither polled nor completed, and the capacity is checked once.
#[test]
fn a_cancel_from_the_capacity_check_after_a_body_event_gets_no_further_body_poll() {
    let (polls, completes, checks, wire) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())], true);
            fence.open.set(true);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            let before = (body.polls.get(), fence.capacity_checks.get());
            *fence.reentry.borrow_mut() = Some(("can_write", Box::new(move || handle.cancel())));
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            let result = run.await;
            fence.reentry.borrow_mut().take();
            (
                body.polls.get() - before.0,
                body.response_completes.get(),
                fence.capacity_checks.get() - before.1,
                wire_of(&state, result),
            )
        })
    });
    assert_eq!(polls, 1, "only the first event is polled");
    assert_eq!(completes, 0, "the response is not completed");
    assert_eq!(
        checks, 1,
        "the capacity is checked once, by the decision whose check cancels"
    );
    assert_eq!(wire.error, Some(HttpErrorCode::CORO_CANCELLED));
    assert_eq!(wire.writes.len(), 1, "only the head is written");
}

/// Waits until `arrived` reaches `target` after adding this thread's arrival; yields now and
/// then, so a round ends on a busy host too.
fn meet(arrived: &AtomicUsize, target: usize) {
    arrived.fetch_add(1, Ordering::AcqRel);
    wait_for(|| arrived.load(Ordering::Acquire) >= target);
}

fn wait_for(done: impl Fn() -> bool) {
    let mut spins = 0u32;
    while !done() {
        spins = spins.wrapping_add(1);
        if spins.is_multiple_of(1024) {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}

/// A xorshift spin of 0 to 7 hints, which staggers two racing threads' starts.
struct Jitter(u32);

impl Jitter {
    fn spin(&mut self) {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        (0..self.0 % 8).for_each(|_| std::hint::spin_loop());
    }
}

/// A wake flag on its own cache line, so that racing flags do not share one.
#[repr(align(128))]
struct Flag(AtomicBool);

/// The driver clears the exchange's wake signal before it polls the body again in its turn
/// (`poll_exchange`), instead of taking it in the next turn. A wake from another thread whose
/// publication the poll after the clear misses must still stand once both are done, so that the
/// driver polls the body again. Each round marks every signal, as a wake during the event's own
/// poll does. Then one thread clears each signal and reads its body's state, while the other
/// publishes that state (a release store, as a lock-free source does) and marks the signal, as
/// the source's wake does. A clear ordered after the mark, together with a read that misses the
/// publication, would lose the wake.
#[test]
fn clearing_a_wake_signal_keeps_a_wake_from_another_thread_whose_publication_the_poll_misses() {
    const ROUNDS: usize = 20_000;
    const SIGNALS: usize = 64;
    let probes: Vec<_> = (0..SIGNALS).map(|_| SignalProbe::new()).collect();
    let published: Arc<Vec<_>> =
        Arc::new((0..SIGNALS).map(|_| Flag(AtomicBool::new(false))).collect());
    let arrived = Arc::new(AtomicUsize::new(0));
    let woken = Arc::new(AtomicUsize::new(0));
    let producer = std::thread::spawn({
        let (probes, published, arrived, woken) = (
            probes.clone(),
            Arc::clone(&published),
            Arc::clone(&arrived),
            Arc::clone(&woken),
        );
        move || {
            let mut jitter = Jitter(0x9e37_79b9);
            for round in 1..=ROUNDS {
                meet(&arrived, 2 * round);
                jitter.spin();
                for (probe, flag) in probes.iter().zip(published.iter()) {
                    flag.0.store(true, Ordering::Release);
                    probe.mark();
                }
                woken.store(round, Ordering::Release);
            }
        }
    });
    let mut jitter = Jitter(0x85eb_ca6b);
    let mut seen = [false; SIGNALS];
    let lost: usize = (1..=ROUNDS)
        .map(|round| {
            for (probe, flag) in probes.iter().zip(published.iter()) {
                probe.mark();
                flag.0.store(false, Ordering::Relaxed);
            }
            meet(&arrived, 2 * round);
            jitter.spin();
            for (seen, (probe, flag)) in seen.iter_mut().zip(probes.iter().zip(published.iter())) {
                probe.clear();
                *seen = flag.0.load(Ordering::Acquire);
            }
            wait_for(|| woken.load(Ordering::Acquire) >= round);
            seen.iter()
                .zip(&probes)
                .filter(|&(&seen, probe)| !seen && !probe.is_ready())
                .count()
        })
        .sum();
    producer.join().unwrap();
    assert_eq!(
        lost, 0,
        "wakes lost in {ROUNDS} rounds of {SIGNALS} signals"
    );
}

/// A body event that becomes ready while the body is polled again after the previous event is
/// polled and written with that event, as when it becomes ready during the next turn's poll.
/// Its producer wakes the waker that the poll left. The poll consumed the earlier wake before it
/// ran, so this wake stands.
#[test]
fn a_body_event_ready_during_the_poll_after_an_event_is_written_with_it() {
    let writes = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            body.push(HttpBodyEvent::body(queue(b"one"), false));
            let producer = Rc::clone(&body);
            *body.on_pending.borrow_mut() = Some(Box::new(move || {
                producer.push(HttpBodyEvent::body(queue(b"two"), false));
            }));
            assert!(run.as_mut().poll(&mut poll).is_pending());
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            // A lost wake stalls the manual host, which then panics.
            run.await.unwrap();
            state.borrow().writes.clone()
        })
    });
    assert_eq!(
        writes[1..],
        [
            b"3\r\none\r\n3\r\ntwo\r\n".to_vec(),
            b"3\r\nend\r\n0\r\n\r\n".to_vec(),
        ]
    );
}

/// Pushes `events` body events of 40 bytes together to a `PushedApp` exchange whose head was
/// written, under a 32-byte write size, and polls the driver once. The exchange ignores the
/// `maximum` each poll offers, which the driver tolerates: each event fills a write on its own.
/// Returns that poll's host yields and the length of every write.
fn oversized_events_in_one_poll(events: usize) -> (usize, Vec<usize>) {
    run_on_host_with(move |host, context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let mut options = owned_options(context, transport);
            options.io.maximum_write_size = 32;
            let (session, handle) = OwnedH1Session::new(application, options).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let before = host.yields();
            for _ in 0..events {
                body.push(HttpBodyEvent::body(queue(&[b'x'; 40]), false));
            }
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let yields = host.yields() - before;
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            let writes = state.borrow().writes.iter().map(Vec::len).collect();
            (yields, writes)
        })
    })
}

/// Body events that each reach the write size are each written before the body is polled
/// again, in the same turn, so they spend the turn budget as recorded below. Polling the body
/// again before such an event's write would spend one more turn per event, and the driver would
/// yield sooner.
#[test]
fn oversized_body_events_ready_together_yield() {
    let recorded: &[(usize, usize, usize)] = &[(16, 0, 29), (21, 0, 37), (22, 0, 38), (32, 1, 53)];
    let actual: Vec<String> = [16, 21, 22, 32]
        .into_iter()
        .map(|events| {
            let (yields, writes) = oversized_events_in_one_poll(events);
            format!("({events}, {yields}, {}),", writes.len())
        })
        .collect();
    let expected: Vec<String> = recorded
        .iter()
        .map(|(events, yields, writes)| format!("({events}, {yields}, {writes}),"))
        .collect();
    assert_eq!(
        actual,
        expected,
        "(events, yields, writes):\n{}",
        actual.join("\n")
    );
}

/// Pushes `events` one-byte body events together to a `PushedApp` exchange whose head was
/// written, and polls the driver once. Returns that poll's driver turns and host yields, and the
/// length of every write.
fn pushed_events_in_one_poll(events: usize) -> (usize, usize, Vec<usize>) {
    run_on_host_with(move |host, context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(PushedBody::default());
            let application = PushedApp {
                body: Rc::clone(&body),
            };
            let (session, handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let mut run = std::pin::pin!(session.run());
            let mut poll = Context::from_waker(Waker::noop());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let before = (driver_turns(), host.yields());
            for _ in 0..events {
                body.push(HttpBodyEvent::body(queue(b"x"), false));
            }
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let (turns, yields) = (driver_turns() - before.0, host.yields() - before.1);
            body.push(HttpBodyEvent::body(queue(b"end"), true));
            assert!(handle.initiate_drain().unwrap());
            run.await.unwrap();
            let writes = state.borrow().writes.iter().map(Vec::len).collect();
            (turns, yields, writes)
        })
    })
}

/// Body events that are ready together are serialized one per turn, each poll of the body
/// after the first counting as a turn even when it runs inside the turn before, and written
/// together once the body is pending. When that write starts in the turn before the budget's
/// last, the budget still runs out after the write's completion is taken, and the driver
/// yields, as when a whole turn took that completion. In the recorded values, 61 events take
/// turns 1 to 61, the write counts as turns 62 and 63, and the completion uses the 64th. One
/// event fewer ends the poll without a yield; one more starts the write in the last turn, which
/// yields immediately.
#[test]
fn a_write_started_at_the_turn_budget_edge_yields() {
    let recorded: &[(usize, usize, &[usize])] = &[
        (60, 0, &[106, 360, 13]),
        (61, 1, &[106, 366, 13]),
        (62, 1, &[106, 372, 13]),
    ];
    let actual: Vec<String> = [60, 61, 62]
        .into_iter()
        .map(|events| {
            let (_, yields, writes) = pushed_events_in_one_poll(events);
            format!("({events}, {yields}, &{writes:?}),")
        })
        .collect();
    let expected: Vec<String> = recorded
        .iter()
        .map(|(events, yields, writes)| format!("({events}, {yields}, &{writes:?}),"))
        .collect();
    assert_eq!(
        actual,
        expected,
        "(events, yields, write lengths):\n{}",
        actual.join("\n")
    );
}

/// A cancel or a drain that the retiring exchange's `complete` requests, when the connection has
/// no other work, ends the connection within the poll that retired the exchange. The driver
/// observes it before returning, without waiting to be polled again.
#[test]
fn a_cancel_or_drain_from_complete_ends_the_connection_in_that_poll() {
    for cancel in [true, false] {
        let (first_poll_ready, wire) = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) =
                    ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
                let (application, _probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let slot = Rc::clone(if cancel {
                    &application.cancel_in
                } else {
                    &application.drain_in
                });
                let (session, handle) =
                    OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
                *slot.borrow_mut() = Some(("complete", handle));
                let mut run = std::pin::pin!(session.run());
                let first = run.as_mut().poll(&mut Context::from_waker(Waker::noop()));
                let ready = first.is_ready();
                let result = match first {
                    Poll::Ready(result) => result,
                    Poll::Pending => run.await,
                };
                slot.borrow_mut().take();
                (ready, wire_of(&state, result))
            })
        });
        let label = if cancel { "cancel" } else { "drain" };
        assert!(
            first_poll_ready,
            "{label}: the connection ends in the poll that retired the exchange"
        );
        assert!(wire.output.ends_with(b"\r\n\r\nhello"), "{label}");
        if cancel {
            assert_eq!(wire.error, Some(HttpErrorCode::CORO_CANCELLED));
            assert_eq!(wire.close, CloseKind::Reset);
        } else {
            assert_eq!(wire.error, None);
            assert_eq!(wire.close, CloseKind::ShutdownWrite);
        }
    }
}

/// The write lengths of the owned driver serving a 16 KiB response, with or without a
/// `Content-Length`, under a 16 KiB write size: its body is polled in pieces that fit beside
/// the queued output.
fn large_response_writes(content_length: bool) -> Vec<usize> {
    let response = Response {
        content_length,
        ..Response::fixed(200, &patterned(16 << 10))
    };
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(CLOSING_GET.to_vec())]);
            let (application, _probes) = H1App::new(vec![response]);
            let mut options = owned_options(context, transport);
            options.io.buffer_limit = 16 << 10;
            options.io.maximum_write_size = 16 << 10;
            options.io.ingress_buffer_limit = 4 << 10;
            options.io.maximum_read_size = 4 << 10;
            options.io.read_allocation_size = 4 << 10;
            let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
            let result = session.run().await;
            wire_of(&state, result).writes
        })
    })
}

/// The write lengths of the owned driver serving three pipelined GETs through an unsaturated
/// write queue, whose exchanges retire at serialization.
fn unsaturated_pipelined_writes() -> Vec<usize> {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(pipelined_gets(3))], true);
            fence.unsaturated.set(true);
            fence.open.set(true);
            let (application, _probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) =
                OwnedH1Session::new(application, owned_options(context, transport)).unwrap();
            let result = session.run().await;
            wire_of(&state, result).writes
        })
    })
}

/// Exact write boundaries for every corpus vector and split, a body polled in pieces,
/// retirement at serialization, and pushed body events.
#[test]
fn owned_h1_writes_in_the_recorded_pieces() {
    let recorded_corpus: &[(&str, [&[usize]; 3])] = &[
        (
            "fragmented_chunked_request_with_trailer",
            [&[99], &[99], &[99]],
        ),
        ("pipelined_requests", [&[99, 99], &[99, 99], &[99, 99]]),
        ("malformed_header", [&[103], &[103], &[103]]),
        ("consumer_shaped_post", [&[139], &[139], &[139]]),
        ("event_stream_response", [&[139], &[139], &[139]]),
        ("head", [&[97], &[97], &[97]]),
        ("no_content", [&[97], &[97], &[97]]),
        ("not_modified", [&[97], &[97], &[97]]),
        ("http10", [&[94], &[94], &[94]]),
        ("http10_keep_alive", [&[99], &[99], &[99]]),
        ("connection_close", [&[94], &[94], &[94]]),
        (
            "two_pipelined_requests",
            [&[101, 100], &[101, 100], &[97, 4, 100]],
        ),
        ("content_length_and_chunked", [&[103], &[103], &[103]]),
        ("duplicate_content_length", [&[103], &[103], &[103]]),
        ("obs_fold", [&[103], &[103], &[103]]),
        ("absolute_form_with_host", [&[99], &[99], &[99]]),
        ("connect", [&[73, 12], &[73, 12], &[73, 12]]),
        ("websocket_upgrade", [&[166, 11], &[166, 11], &[166, 11]]),
        ("expect_continue", [&[153], &[153], &[153]]),
        ("chunked_with_trailers", [&[99], &[99], &[99]]),
        ("head_at_the_limit", [&[94], &[94], &[92, 2]]),
        ("head_over_the_limit", [&[123], &[123], &[123]]),
        ("malformed_request_line", [&[103], &[103], &[103]]),
    ];
    let recorded_others: &[(&str, &[usize])] = &[
        ("large response with Content-Length", &[16367, 113]),
        ("large chunked response", &[16375, 129]),
        ("unsaturated pipelined GETs", &[301]),
        ("pushed body events", &[106, 8, 8, 10, 13]),
    ];
    let mut actual = Vec::new();
    for vector in corpus() {
        let writes = [Split::Whole, Split::Every7, Split::OneByte].map(|split| {
            corpus_session(
                true,
                &vector,
                split,
                Http1DriverConfig::default(),
                h1_config(),
            )
            .0
            .writes
        });
        actual.push(format!("(\"{}\", {writes:?}),", vector.name));
    }
    let pushed = pushed_body_turns(&[b"one", b"two", b"three"])
        .1
        .iter()
        .map(Vec::len)
        .collect();
    for (name, writes) in [
        (
            "large response with Content-Length",
            large_response_writes(true),
        ),
        ("large chunked response", large_response_writes(false)),
        ("unsaturated pipelined GETs", unsaturated_pipelined_writes()),
        ("pushed body events", pushed),
    ] {
        actual.push(format!("(\"{name}\", &{writes:?}),"));
    }
    let expected: Vec<String> = recorded_corpus
        .iter()
        .map(|(name, writes)| format!("(\"{name}\", {writes:?}),"))
        .chain(
            recorded_others
                .iter()
                .map(|(name, writes)| format!("(\"{name}\", &{writes:?}),")),
        )
        .collect();
    assert_eq!(actual, expected, "write lengths:\n{}", actual.join("\n"));
}

/// A response body of one-byte chunks, each ready when polled once `start` is called; the
/// `cancel_at`th ready poll cancels the session through `handle`.
struct BurstBody {
    started: Cell<bool>,
    polls: Cell<usize>,
    cancel_at: usize,
    handle: RefCell<Option<OwnedH1Handle>>,
    waker: RefCell<Option<Waker>>,
}

impl BurstBody {
    fn start(&self) {
        self.started.set(true);
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Responds to each request immediately with a chunked `200` head, then with `body`.
struct BurstApp {
    body: Rc<BurstBody>,
}

struct BurstExchange {
    head: Option<HttpHeaderEvent>,
    body: Rc<BurstBody>,
}

impl OwnedApplication for BurstApp {
    type Exchange = BurstExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> BurstExchange {
        let mut message = HttpMessage::new();
        message.set_status_code(200).unwrap();
        message.set_http_version(1, 1);
        message.headers_mut().set(HttpHeaderCode::Date, DATE);
        BurstExchange {
            head: Some(HttpHeaderEvent::new(message, false).unwrap()),
            body: Rc::clone(&self.body),
        }
    }
}

impl OwnedExchange for BurstExchange {
    fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {}

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        Poll::Ready(Ok(self
            .head
            .take()
            .expect("the response head is polled once")))
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if !self.body.started.get() {
            *self.body.waker.borrow_mut() = Some(context.waker().clone());
            return Poll::Pending;
        }
        let polls = self.body.polls.get() + 1;
        self.body.polls.set(polls);
        if polls == self.body.cancel_at {
            self.body
                .handle
                .borrow()
                .as_ref()
                .expect("the handle is set before the body starts")
                .cancel();
        }
        Poll::Ready(Ok(HttpBodyEvent::body(queue(b"x"), false)))
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

/// The write lengths of the owned driver that, under a 64-byte write size and with writes that
/// complete immediately, writes a chunked response head, then streams one-byte chunks, each
/// ready when polled, until the `cancel_at`th body poll cancels the connection.
fn burst_writes(cancel_at: usize) -> Vec<usize> {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let body = Rc::new(BurstBody {
                started: Cell::new(false),
                polls: Cell::new(0),
                cancel_at,
                handle: RefCell::new(None),
                waker: RefCell::new(None),
            });
            let application = BurstApp {
                body: Rc::clone(&body),
            };
            let mut options = owned_options(context, transport);
            options.io.maximum_write_size = 64;
            let (session, handle) = OwnedH1Session::new(application, options).unwrap();
            *body.handle.borrow_mut() = Some(handle);
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "the response waits for its body"
            );
            body.start();
            let result = run.await;
            body.handle.borrow_mut().take();
            let wire = wire_of(&state, result);
            assert_eq!(wire.error, Some(HttpErrorCode::CORO_CANCELLED));
            wire.writes
        })
    })
}

/// A burst of ready response body events that runs past the turn budget, and a cancel that
/// discards what is not yet written, write the recorded pieces. Do not change them. A turn that
/// is skipped because no phase has work still counts against the budget, so the write that the
/// budget forces at its last turn holds the recorded bytes.
#[test]
fn a_cancelled_body_burst_past_the_turn_budget_writes_the_recorded_pieces() {
    let recorded: &[(usize, &[usize])] = &[
        (55, &[64, 42, 48, 48, 48, 48, 48, 48, 24]),
        (56, &[64, 42, 48, 48, 48, 48, 48, 48, 24]),
        (57, &[64, 42, 48, 48, 48, 48, 48, 48, 24]),
        (60, &[64, 42, 48, 48, 48, 48, 48, 48, 24]),
        (61, &[64, 42, 48, 48, 48, 48, 48, 48, 24, 48]),
        (62, &[64, 42, 48, 48, 48, 48, 48, 48, 24, 48]),
        (
            110,
            &[
                64, 42, 48, 48, 48, 48, 48, 48, 24, 48, 48, 48, 48, 48, 48, 24,
            ],
        ),
        (
            118,
            &[
                64, 42, 48, 48, 48, 48, 48, 48, 24, 48, 48, 48, 48, 48, 48, 24, 48,
            ],
        ),
        (
            119,
            &[
                64, 42, 48, 48, 48, 48, 48, 48, 24, 48, 48, 48, 48, 48, 48, 24, 48,
            ],
        ),
        (
            120,
            &[
                64, 42, 48, 48, 48, 48, 48, 48, 24, 48, 48, 48, 48, 48, 48, 24, 48,
            ],
        ),
    ];
    let actual: Vec<String> = [55, 56, 57, 60, 61, 62, 110, 118, 119, 120]
        .into_iter()
        .map(|cancel_at| format!("({cancel_at}, &{:?}),", burst_writes(cancel_at)))
        .collect();
    let expected: Vec<String> = recorded
        .iter()
        .map(|(cancel_at, writes)| format!("({cancel_at}, &{writes:?}),"))
        .collect();
    assert_eq!(actual, expected, "write lengths:\n{}", actual.join("\n"));
}

/// The response body of the TCP early-retirement tests, sent as one transport write that the
/// shrunken loopback buffers cannot hold while the client does not read.
const TCP_BODY: usize = 2 << 20;
/// `SO_SNDBUF` of the session's socket and `SO_RCVBUF` of the client's.
const TCP_SOCKET_BUFFER: u32 = 64 << 10;
/// The session's write and buffer bound, so that one write carries the whole response.
const TCP_WRITE_LIMIT: usize = 4 << 20;

/// What the client and the owned HTTP/1.1 session observed in `h1_retirement_over_tcp`.
struct TcpRetirement {
    /// The exchange's `complete()` ran before the client read anything.
    retired_before_read: bool,
    run: Result<(), HttpErrorCode>,
    /// (`response_complete`, `complete`) counts and the cancels of the exchange.
    exchange: (usize, usize, Vec<(HttpErrorCode, bool)>),
    received: Vec<u8>,
}

/// Serves one GET with owned HTTP/1.1 over an accepted plaintext TCP `OwnedByteStream`, whose
/// native write queue reports `is_saturated`, with a `TCP_BODY`-byte response. The client does
/// not read until the exchange's `complete()` has run, or for 5 seconds. Then it reads the
/// whole response and ends the connection, or, when `reset_after_complete`, resets it.
fn h1_retirement_over_tcp(reset_after_complete: bool) -> TcpRetirement {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(tokio::task::LocalSet::new().run_until(async move {
            let listener = tokio::net::TcpSocket::new_v4().unwrap();
            listener.set_send_buffer_size(TCP_SOCKET_BUFFER).unwrap();
            listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = listener.listen(1).unwrap();
            let client = tokio::net::TcpSocket::new_v4().unwrap();
            client.set_recv_buffer_size(TCP_SOCKET_BUFFER).unwrap();
            let (client, accepted) = tokio::join!(
                client.connect(listener.local_addr().unwrap()),
                listener.accept()
            );
            let mut client = client.unwrap();
            let accepted = accepted.unwrap().0.into_std().unwrap();
            let (worker, driver) = roxygen_runtime::driver::WorkerDriver::new(16).unwrap();
            let call = worker
                .call_local(move |context| async move {
                    let transport: Rc<dyn UniplexTransport> = Rc::new(
                        roxygen_transport::tcp::TcpTransport::from_accepted(
                            &context,
                            accepted,
                            roxygen_transport::byte_stream::ByteStreamLimits::new(
                                64 << 10,
                                TCP_WRITE_LIMIT,
                                8,
                            )
                            .unwrap(),
                        )
                        .unwrap(),
                    );
                    let (application, probes) =
                        H1App::new(vec![Response::fixed(200, &vec![b'r'; TCP_BODY])]);
                    let mut options = owned_options(context, transport);
                    options.io.buffer_limit = TCP_WRITE_LIMIT;
                    options.io.maximum_write_size = TCP_WRITE_LIMIT;
                    let (session, _handle) = OwnedH1Session::new(application, options).unwrap();
                    let client_probes = Rc::clone(&probes);
                    let client_task = tokio::task::spawn_local(async move {
                        client
                            .write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n")
                            .await
                            .unwrap();
                        let retired_before_read =
                            tokio::time::timeout(Duration::from_secs(5), async {
                                while client_probes
                                    .borrow()
                                    .first()
                                    .is_none_or(|probe| probe.completed.get() == 0)
                                {
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                            })
                            .await
                            .is_ok();
                        let mut received = Vec::new();
                        if reset_after_complete {
                            client.set_zero_linger().unwrap();
                            drop(client);
                            return (retired_before_read, received);
                        }
                        let mut buffer = vec![0; 64 << 10];
                        while received
                            .windows(4)
                            .position(|window| window == b"\r\n\r\n")
                            .is_none_or(|head| received.len() < head + 4 + TCP_BODY)
                        {
                            let read = client.read(&mut buffer).await.unwrap();
                            assert_ne!(read, 0, "the session closed before its response ended");
                            received.extend_from_slice(&buffer[..read]);
                        }
                        client.shutdown().await.unwrap();
                        client.read_to_end(&mut received).await.unwrap();
                        (retired_before_read, received)
                    });
                    let (run, client) =
                        tokio::join!(tokio::task::spawn_local(session.run()), client_task);
                    let run = run
                        .unwrap_or_else(|failure| std::panic::resume_unwind(failure.into_panic()));
                    let (retired_before_read, received) = client
                        .unwrap_or_else(|failure| std::panic::resume_unwind(failure.into_panic()));
                    let probe = Rc::clone(&probes.borrow()[0]);
                    TcpRetirement {
                        retired_before_read,
                        run: run.map(drop).map_err(|error| error.code()),
                        exchange: (
                            probe.response_complete.get(),
                            probe.completed.get(),
                            probe.cancelled.borrow().clone(),
                        ),
                        received,
                    }
                })
                .unwrap();
            let driver_task = tokio::task::spawn_local(driver);
            let result = tokio::time::timeout(Duration::from_secs(30), call)
                .await
                .expect("the TCP exchange finishes")
                .unwrap();
            worker.begin_shutdown();
            driver_task.await.unwrap();
            result
        }))
}

/// On plaintext TCP the native write queue reports `is_saturated`, so an exchange can retire at
/// serialization. It retires once its response's end is serialized and handed to the queue,
/// and its `complete()` runs before the client reads any of the one write that carries it.
#[test]
fn owned_h1_retires_a_response_at_serialization_over_tcp() {
    let outcome = h1_retirement_over_tcp(false);
    assert!(
        outcome.retired_before_read,
        "the exchange retires at its response's serialization, before the client reads"
    );
    assert_eq!(outcome.run, Ok(()));
    assert_eq!(outcome.exchange, (1, 1, Vec::new()));
    let head = outcome
        .received
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head")
        + 4;
    assert!(
        outcome.received.starts_with(b"HTTP/1.1 200 "),
        "{:?}",
        String::from_utf8_lossy(&outcome.received[..head])
    );
    assert_eq!(outcome.received.len(), head + TCP_BODY);
    assert!(outcome.received[head..].iter().all(|byte| *byte == b'r'));
}

/// A write failure after an early retirement on TCP fails the connection, and does not reach the
/// retired exchange. The client resets after `complete()`, the run fails, and the exchange does
/// not get a `cancel`.
#[test]
fn a_write_failure_after_an_early_retirement_over_tcp_fails_only_the_connection() {
    let outcome = h1_retirement_over_tcp(true);
    assert!(
        outcome.retired_before_read,
        "the exchange retires at its response's serialization, before the client reads"
    );
    assert!(outcome.run.is_err(), "the reset fails the connection");
    assert_eq!(outcome.exchange, (1, 1, Vec::new()));
}

type DeadlineCase = fn(Rc<ManualHost>, WorkerContext) -> Pin<Box<dyn Future<Output = ()>>>;

/// Runs `case` on a host that drives a wheel timer and on one where the driver's deadline falls
/// back to host sleeps.
fn on_both_hosts(case: DeadlineCase) {
    run_on_wheel_host_with(case);
    run_on_host_with(case);
}

/// Lets the driver run until it waits.
async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

/// The handshake deadline of the connection-deadline tests.
const DEADLINE_HANDSHAKE: Duration = Duration::from_secs(8);

/// The owned session's options on `transport`, applying `deadlines`.
fn deadline_options(
    context: WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    deadlines: OwnedConnectionDeadlines,
) -> OwnedH1Options {
    let mut options = owned_options(context, transport);
    options.http1.deadlines = deadlines;
    options
}

/// A request head that has not ended by the handshake deadline resets the connection then,
/// with no response, and its exchange is cancelled with `READ_TIMEOUT`.
#[test]
fn h1_head_deadline_closes_without_response() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: a\r\n".to_vec(),
            )]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        DEADLINE_HANDSHAKE,
                        Duration::ZERO,
                        DeadlineExpiry::HardClose,
                    ),
                ),
            )
            .unwrap();
            let (result, ()) = futures::join!(session.run(), async {
                settle().await;
                assert_eq!(
                    probes.borrow().len(),
                    1,
                    "the request line opened an exchange"
                );
                host.advance(DEADLINE_HANDSHAKE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the deadline fired on time"
                );
            });
            assert_eq!(
                result
                    .expect_err("a head expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert!(state.borrow().writes.is_empty(), "no response is written");
            assert_eq!(
                *probes.borrow()[0].cancelled.borrow(),
                [(HttpErrorCode::READ_TIMEOUT, false)]
            );
        })
    });
}

/// The idle deadline of the connection-deadline tests.
const DEADLINE_IDLE: Duration = Duration::from_secs(10);

/// An accepted CONNECT tunnel stays in the idle phase, not a head wait: tunnel bytes read at
/// half the idle timeout move its expiry from `T` to `1.5 T`, past the handshake deadline, and
/// the idle tunnel then resets with nothing written after the response head.
#[test]
fn h1_upgraded_tunnel_idle_closes_without_bytes() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let tunnel = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(CONNECT_HEAD.to_vec()),
                ReadAction::WaitData(Arc::clone(&tunnel), vec![b'x'; 10]),
            ]);
            let (application, probes) = H1App::new(vec![Response::tunnel(200, b"y", false)]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        DEADLINE_HANDSHAKE,
                        DEADLINE_IDLE,
                        DeadlineExpiry::HardClose,
                    ),
                ),
            )
            .unwrap();
            let (result, written) = futures::join!(session.run(), async {
                settle().await;
                let written = state.borrow().writes.concat();
                assert!(
                    written.starts_with(b"HTTP/1.1 200") && written.ends_with(b"\r\n\r\n"),
                    "the tunnel's response head is written: {:?}",
                    String::from_utf8_lossy(&written)
                );
                host.advance(DEADLINE_IDLE / 2);
                tunnel.notify_one();
                settle().await;
                assert_eq!(*probes.borrow()[0].request_body.borrow(), [b'x'; 10]);
                host.advance(DEADLINE_HANDSHAKE - DEADLINE_IDLE / 2);
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "a tunnel is not a head wait"
                );
                host.advance(DEADLINE_IDLE - DEADLINE_HANDSHAKE);
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the tunnel read restarted the idle deadline"
                );
                host.advance(DEADLINE_IDLE / 2 - Duration::from_millis(1));
                settle().await;
                assert_eq!(state.borrow().close, CloseKind::Open);
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the deadline fired on time"
                );
                written
            });
            assert_eq!(
                result
                    .expect_err("an idle expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(
                state.borrow().writes.concat(),
                written,
                "nothing is written after the response head"
            );
            assert_eq!(
                *probes.borrow()[0].cancelled.borrow(),
                [(HttpErrorCode::READ_TIMEOUT, false)]
            );
        })
    });
}

/// Zero deadlines cost nothing: a full exchange on a host without a wheel allocates no
/// `DeadlineTimer` and creates no host timer. With an idle deadline set, the same exchange on a
/// connection that then stays open allocates one timer, which sleeps on the host and expires.
#[test]
fn zero_deadlines_allocate_nothing() {
    fn exchange(
        deadlines: OwnedConnectionDeadlines,
        reads: Vec<ReadAction>,
    ) -> (usize, usize, Option<HttpErrorCode>) {
        run_on_host_with(move |host, context| {
            Box::pin(async move {
                let timers = deadline_timers();
                let (transport, state) = ScriptedTransport::new(reads);
                let (application, _probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
                let (session, _handle) = OwnedH1Session::new(
                    application,
                    deadline_options(context, transport, deadlines),
                )
                .unwrap();
                let result = session.run().await;
                assert!(
                    state.borrow().writes.concat().ends_with(b"hello"),
                    "the exchange completed"
                );
                (
                    deadline_timers() - timers,
                    host.timers_created(),
                    result.err().map(|error| error.code()),
                )
            })
        })
    }
    assert_eq!(
        exchange(
            OwnedConnectionDeadlines::default(),
            vec![ReadAction::Data(KEEP_ALIVE_GET.to_vec()), ReadAction::Eof],
        ),
        (0, 0, None),
        "zero deadlines allocate no timer and create no host timer"
    );
    let (timers, host_timers, error) = exchange(
        OwnedConnectionDeadlines::new(Duration::ZERO, DEADLINE_IDLE, DeadlineExpiry::HardClose),
        vec![ReadAction::Data(KEEP_ALIVE_GET.to_vec())],
    );
    assert_eq!((timers, error), (1, Some(HttpErrorCode::READ_TIMEOUT)));
    assert!(host_timers > 0, "an armed deadline sleeps on the host");
}

/// Each wait for a request head gets its own head deadline, measured from the start of that
/// wait. The first request arrives at `H/2`; the next head trickles in every `H/4`, each part
/// within the idle timeout `H/2`, and never ends. The connection is open at `H`, when the first
/// wait's deadline would have passed, and resets with no second response at `1.5 H`.
#[test]
fn h1_head_deadline_rearms_for_each_keep_alive_request() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let gates: Vec<_> = (0..4)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let parts: [&[u8]; 4] = [KEEP_ALIVE_GET, b"GET / HT", b"TP/1.1\r\n", b"Host: a\r\n"];
            let (transport, state) = ScriptedTransport::new(
                gates
                    .iter()
                    .zip(parts)
                    .map(|(gate, part)| ReadAction::WaitData(Arc::clone(gate), part.to_vec())),
            );
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        DEADLINE_HANDSHAKE,
                        DEADLINE_HANDSHAKE / 2,
                        DeadlineExpiry::HardClose,
                    ),
                ),
            )
            .unwrap();
            let (result, first) = futures::join!(session.run(), async {
                settle().await;
                host.advance(DEADLINE_HANDSHAKE / 2);
                gates[0].notify_one();
                settle().await;
                let first = state.borrow().writes.concat();
                assert!(first.ends_with(b"hello"), "the first request is served");
                for gate in &gates[1..] {
                    host.advance(DEADLINE_HANDSHAKE / 4);
                    gate.notify_one();
                    settle().await;
                    assert_eq!(
                        state.borrow().close,
                        CloseKind::Open,
                        "each head wait has its own deadline, and each part restarts idle"
                    );
                }
                host.advance(DEADLINE_HANDSHAKE / 4 - Duration::from_millis(1));
                settle().await;
                assert_eq!(state.borrow().close, CloseKind::Open);
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the second head wait's deadline passed"
                );
                first
            });
            assert_eq!(
                result
                    .expect_err("a head expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(state.borrow().writes.concat(), first, "no second response");
            let probes = probes.borrow();
            assert_eq!(probes.len(), 2);
            assert!(probes[0].cancelled.borrow().is_empty());
            assert_eq!(
                *probes[1].cancelled.borrow(),
                [(HttpErrorCode::READ_TIMEOUT, false)]
            );
        })
    });
}

/// With `Drain`, an expiry on a connection that waits for its next request closes it
/// gracefully at once, with nothing more written. On a second connection, an exchange whose
/// response made no progress for the whole idle timeout is aborted at the expiry, as with
/// `HardClose`, with no hold.
#[test]
fn h1_drain_expiry_closes_idle_and_aborts_live_exchange() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        Duration::ZERO,
                        DEADLINE_IDLE,
                        DeadlineExpiry::Drain,
                    ),
                ),
            )
            .unwrap();
            let (result, first) = futures::join!(session.run(), async {
                settle().await;
                let first = state.borrow().writes.concat();
                assert!(first.ends_with(b"hello"), "the request is served");
                host.advance(DEADLINE_IDLE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::ShutdownWrite,
                    "the idle connection closes gracefully at the expiry"
                );
                first
            });
            result.expect("an idle drain closes cleanly");
            assert_eq!(
                state.borrow().writes.concat(),
                first,
                "nothing more is written"
            );
            assert_eq!(probes.borrow()[0].completed.get(), 1);
            assert!(probes.borrow()[0].cancelled.borrow().is_empty());
        })
    });
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())]);
            let (mut application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            application.response_resume = Some(Rc::new(ResumeProbe::default()));
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        Duration::ZERO,
                        DEADLINE_IDLE,
                        DeadlineExpiry::Drain,
                    ),
                ),
            )
            .unwrap();
            let (result, head) = futures::join!(session.run(), async {
                settle().await;
                let head = state.borrow().writes.concat();
                assert!(
                    head.starts_with(b"HTTP/1.1 200") && head.ends_with(b"\r\n\r\n"),
                    "the response head is written and its body waits: {:?}",
                    String::from_utf8_lossy(&head)
                );
                host.advance(DEADLINE_IDLE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the live exchange is aborted at the expiry"
                );
                head
            });
            assert_eq!(
                result
                    .expect_err("a live exchange's expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(
                state.borrow().writes.concat(),
                head,
                "nothing more is written"
            );
            assert_eq!(
                *probes.borrow()[0].cancelled.borrow(),
                [(HttpErrorCode::READ_TIMEOUT, false)]
            );
        })
    });
}

/// With `Drain`, a connection waiting for its next request whose last response is still
/// unwritten has made no write progress for the whole idle timeout, so the expiry hard-closes
/// it rather than wait for that output. The exchange retired when its response was serialized,
/// through an unsaturated write queue whose writes stay incomplete.
#[test]
fn h1_drain_expiry_with_stuck_output_hard_closes() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())], true);
            fence.unsaturated.set(true);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        Duration::ZERO,
                        DEADLINE_IDLE,
                        DeadlineExpiry::Drain,
                    ),
                ),
            )
            .unwrap();
            let (result, ()) = futures::join!(session.run(), async {
                settle().await;
                assert!(
                    state.borrow().writes.concat().ends_with(b"hello"),
                    "the response is handed to the queue"
                );
                assert_eq!(
                    probes.borrow()[0].completed.get(),
                    1,
                    "the exchange retired at serialization"
                );
                host.advance(DEADLINE_IDLE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "stuck output is not waited for"
                );
                fence.open();
            });
            assert_eq!(
                result.expect_err("the expiry fails the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
        })
    });
}

/// A head wait starts once the previous response is written. The exchange retires when its
/// response is serialized, through an unsaturated write queue, and the write completes at
/// `H + 1 s`, within the idle timeout. The connection is open then, and resets `H` after the
/// write completed.
#[test]
fn h1_head_deadline_starts_after_response_is_written() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let (transport, state, fence) =
                FencedTransport::new([ReadAction::Data(KEEP_ALIVE_GET.to_vec())], true);
            fence.unsaturated.set(true);
            let (application, probes) = H1App::new(vec![Response::fixed(200, b"hello")]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        DEADLINE_HANDSHAKE,
                        DEADLINE_IDLE,
                        DeadlineExpiry::HardClose,
                    ),
                ),
            )
            .unwrap();
            let (result, ()) = futures::join!(session.run(), async {
                settle().await;
                assert_eq!(
                    probes.borrow()[0].completed.get(),
                    1,
                    "the exchange retired at serialization"
                );
                host.advance(DEADLINE_HANDSHAKE + Duration::from_secs(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the head wait starts once the response is written"
                );
                fence.open();
                settle().await;
                host.advance(DEADLINE_HANDSHAKE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the head wait's deadline passed"
                );
            });
            assert_eq!(
                result
                    .expect_err("a head expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert!(state.borrow().writes.concat().ends_with(b"hello"));
        })
    });
}

/// A head wait that begins inside the driver poll that served the previous request still gets
/// its own head deadline. The first request is served at `0` and the second at `3H/4`, each
/// read, delivered, answered, written and retired within one poll. The connection is open at
/// `H`, where the first wait's deadline was, and resets at `7H/4`, `H` after the second wait
/// began.
#[test]
fn h1_head_deadline_rearms_after_a_request_served_in_one_poll() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let second = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(KEEP_ALIVE_GET.to_vec()),
                ReadAction::WaitData(Arc::clone(&second), KEEP_ALIVE_GET.to_vec()),
            ]);
            let (application, probes) = H1App::new(vec![
                Response::fixed(200, b"hello"),
                Response::fixed(200, b"hello"),
            ]);
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        DEADLINE_HANDSHAKE,
                        DEADLINE_IDLE * 10,
                        DeadlineExpiry::HardClose,
                    ),
                ),
            )
            .unwrap();
            let (result, served) = futures::join!(session.run(), async {
                settle().await;
                host.advance(DEADLINE_HANDSHAKE * 3 / 4);
                second.notify_one();
                settle().await;
                let served = state.borrow().writes.concat();
                assert_eq!(
                    String::from_utf8_lossy(&served).matches("hello").count(),
                    2,
                    "both requests are served"
                );
                host.advance(DEADLINE_HANDSHAKE / 4);
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the second head wait has its own deadline"
                );
                host.advance(DEADLINE_HANDSHAKE * 3 / 4 - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Open,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the second head wait's deadline passed"
                );
                served
            });
            assert_eq!(
                result
                    .expect_err("a head expiry fails the connection")
                    .code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(
                state.borrow().writes.concat(),
                served,
                "nothing more is written"
            );
            let probes = probes.borrow();
            assert_eq!(probes.len(), 2);
            assert!(
                probes
                    .iter()
                    .all(|probe| probe.cancelled.borrow().is_empty())
            );
            assert!(probes.iter().all(|probe| probe.completed.get() == 1));
        })
    });
}

/// With `Drain`, an expiry during RFC 9112's staged close of a stopped upload fails the
/// connection, as `HardClose` does, instead of leaving the close to read until a peer EOF that
/// may never come. The response is written and the write side shut down, and the peer then sends
/// nothing for the whole idle timeout.
#[test]
fn h1_drain_expiry_aborts_a_staged_close() {
    on_both_hosts(|host, context| {
        Box::pin(async move {
            let rest = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(OPEN_UPLOAD.to_vec()),
                ReadAction::WaitData(Arc::clone(&rest), UPLOAD_END.to_vec()),
            ]);
            let (mut application, probes) = H1App::new(vec![trailered_response()]);
            application.stop_request = Some((HttpErrorCode::NO_ERROR, StopAt::Head));
            let (session, _handle) = OwnedH1Session::new(
                application,
                deadline_options(
                    context,
                    transport,
                    OwnedConnectionDeadlines::new(
                        Duration::ZERO,
                        DEADLINE_IDLE,
                        DeadlineExpiry::Drain,
                    ),
                ),
            )
            .unwrap();
            let (result, served) = futures::join!(session.run(), async {
                settle().await;
                let served = state.borrow().writes.concat();
                assert!(
                    String::from_utf8_lossy(&served).ends_with(TRAILERED_END),
                    "the response is written: {:?}",
                    String::from_utf8_lossy(&served)
                );
                assert_eq!(
                    state.borrow().close,
                    CloseKind::ShutdownWrite,
                    "the staged close shut the write side down and still reads"
                );
                host.advance(DEADLINE_IDLE - Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::ShutdownWrite,
                    "the deadline fired early"
                );
                host.advance(Duration::from_millis(1));
                settle().await;
                assert_eq!(
                    state.borrow().close,
                    CloseKind::Reset,
                    "the expiry ends the staged close"
                );
                rest.notify_one();
                served
            });
            assert_eq!(
                result.expect_err("the expiry fails the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(
                state.borrow().writes.concat(),
                served,
                "nothing more is written"
            );
            assert_eq!(probes.borrow()[0].completed.get(), 1);
        })
    });
}
