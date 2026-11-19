/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Uri;
use http::Version;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http_adapter::HttpAdapterError;
use roxygen_http_adapter::NetworkHeadContext;
use roxygen_http_adapter::UnsupportedMessageMetadata;
use roxygen_http_adapter::request_head_to_http;
use roxygen_hyper_adapter::body::LocalBodyOptions;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyOptions;
use roxygen_hyper_adapter::source::ApplicationBodySource;
use roxygen_runtime::context::ContextEnvelope;
use roxygen_runtime::source_proxy::SourceAbortHandle;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::connection::ServeTransportOptions;
use roxygen_server::connection::serve_transport;
use roxygen_transport::established::AdoptedIoControl;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local_with_completion;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::oneshot;

use super::Admission;
use super::ExchangeState;
use super::Http1ConnectionSwitchRejection;
use super::Mode;
use super::Outcomes;
use super::ServerError;
use super::ServerExchange;
use super::ServerFailure;
use super::ServerOptions;
use super::Signals;
use super::SlotId;
use super::application::AppLeafGuard;
use super::application::Dispatch;
use super::application::Packet;
use super::observe;
use super::panic_error;

pub(super) struct Handoff<Io> {
    pub input: Option<EstablishedIo<Io>>,
    pub claimed: bool,
    pub io: Option<AdoptedIoControl>,
    pub scope: Option<WorkerCleanupObserver>,
}

struct ConnectionGuard {
    signals: Arc<Signals>,
    completed: bool,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.signals.publish_native(Err(Arc::new(ServerError::new(
                ServerFailure::Cleanup("native connection owner lost"),
                false,
            ))));
            self.signals.change(Mode::Cancel);
        }
    }
}

pub(super) fn bootstrap<Io>(
    context: &WorkerContext,
    handoff: Arc<Mutex<Handoff<Io>>>,
    admission: Arc<Admission>,
    options: ServerOptions,
) -> Result<Arc<ConnectionContext>, ServerError>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    let signals = Arc::clone(&admission.signals);
    if signals.mode() != Mode::Open || !context.handle().is_accepting() {
        return Err(ServerError::new(ServerFailure::Admission, true));
    }
    let (sender, receiver) = oneshot::channel::<roxygen_server::connection::ServerConnection>();
    let guard = ConnectionGuard {
        signals: Arc::clone(&signals),
        completed: false,
    };
    let registration = context.register_scope(async move {
        let mut guard = guard;
        let Ok(connection) = receiver.await else { return; };
        let mut closed = connection.wait_closed();
        let mut modes = guard.signals.mode.subscribe();
        let mut outcomes = Outcomes::default();
        let mut applied_mode = Mode::Open;
        let result = loop {
            let mode = *modes.borrow_and_update();
            tokio::select! {
                biased;
                result = &mut closed => break result.map(|_| ()).map_err(|error| {
                    Arc::new(ServerError::new(ServerFailure::Native(error), true))
                }),
                () = std::future::ready(()), if mode > applied_mode => {
                    applied_mode = mode;
                    match mode {
                        Mode::Open => {},
                        Mode::Drain => {
                            if let Some(Err(error)) = guard.signals.faults.capture(|| connection.initiate_drain()) {
                                let error = Arc::new(ServerError::new(ServerFailure::Native(error), true));
                                guard.signals.faults.record(Err(Arc::clone(&error)));
                                outcomes.record(Err(error));
                                guard.signals.change(Mode::Cancel);
                            }
                        },
                        Mode::Cancel => { guard.signals.faults.capture(|| connection.cancel()); },
                    }
                },
                changed = modes.changed() => {
                    if changed.is_err() { guard.signals.change(Mode::Cancel); }
                }
            }
        };
        outcomes.record(result);
        let result = outcomes.result();
        guard.completed = true;
        guard.signals.publish_native(result.clone());
        guard.signals.change(if result.is_ok() { Mode::Drain } else { Mode::Cancel });
    }).map_err(|_| ServerError::new(ServerFailure::Admission, true))?;
    handoff
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .scope = Some(registration.observe());
    drop(registration.activate().map_err(|_| {
        ServerError::new(ServerFailure::Cleanup("connection scope activation"), false)
    })?);
    let established = {
        let mut handoff = handoff.lock().unwrap_or_else(|error| error.into_inner());
        if signals.mode() != Mode::Open {
            return Err(ServerError::new(ServerFailure::Cancelled, true));
        }
        let input = handoff.input.take().expect("single bootstrap claims input");
        handoff.claimed = true;
        input
    };
    let (transport, io) =
        adopt_local_with_completion(context, established, options.session.byte_stream_limits)
            .map_err(|error| ServerError::new(ServerFailure::Native(error), false))?;
    handoff.lock().unwrap_or_else(|error| error.into_inner()).io = Some(io);
    let handler = Rc::new(FacadeHandler {
        context: context.clone(),
        admission,
        request_body: options.request_body,
        response_body: options.response_body,
        protocol: options.protocol,
        rejection: options.http1_connection_switch_rejection,
    });
    let mut native_options = ServeTransportOptions::new(options.protocol, options.security);
    native_options.session = options.session;
    native_options.upstream_origin = options.upstream_origin;
    let native = serve_transport(context, transport, handler, native_options)
        .map_err(|error| ServerError::new(ServerFailure::Native(error), true))?;
    let trusted = Arc::clone(native.connection_context());
    sender.send(native).map_err(|_| {
        ServerError::new(ServerFailure::Cleanup("native connection handoff"), false)
    })?;
    Ok(trusted)
}

struct FacadeHandler {
    context: WorkerContext,
    admission: Arc<Admission>,
    request_body: LocalBodyOptions,
    response_body: ApplicationBodyOptions,
    protocol: CodecProtocol,
    rejection: Option<Http1ConnectionSwitchRejection>,
}

/// Checks every request conversion constraint except the protocol-switch ones,
/// without clearing native metadata. Keep this in sync with
/// `request_head_to_http`.
fn rejection_version(message: &HttpMessage) -> Option<Version> {
    if !message.is_request()
        || message.is_egress_websocket_upgrade()
        || message.upgrade_protocol().is_some()
        || message.is_masque()
        || message.trailers().is_some()
    {
        return None;
    }
    let version = match message.http_version() {
        (1, 0) => Version::HTTP_10,
        (1, 1) => Version::HTTP_11,
        _ => return None,
    };
    let mut message = message.clone();
    let method = Method::from_bytes(message.method_string().ok()?.as_bytes()).ok()?;
    let upgrade = message.headers().exists(HttpHeaderCode::Upgrade);
    if method != Method::CONNECT && !upgrade {
        return None;
    }
    if message.is_upgraded() && method != Method::CONNECT {
        return None;
    }
    if message.is_ingress_websocket_upgrade()
        && !message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Upgrade)
            .eq_ignore_ascii_case(b"websocket")
    {
        return None;
    }
    let uri = Uri::try_from(message.url().ok()?).ok()?;
    if let Some(scheme) = uri.scheme_str()
        && (!matches!(scheme, "http" | "https") || message.scheme() != scheme.as_bytes())
    {
        return None;
    }
    let mut headers = HeaderMap::new();
    for (_, name, value) in message.headers().iter() {
        headers
            .try_append(
                HeaderName::from_bytes(name).ok()?,
                HeaderValue::from_bytes(value).ok()?,
            )
            .ok()?;
    }
    Some(version)
}

struct WatchGuard(Arc<ExchangeState>);
impl Drop for WatchGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct HandlerGuard {
    state: Arc<ExchangeState>,
    returned: bool,
}

struct PartialPacket {
    admission: Arc<Admission>,
    id: SlotId,
    packet: Option<Packet>,
}

impl PartialPacket {
    fn transfer(&mut self) {
        if let Some(packet) = self.packet.take() {
            self.admission.dispatch(self.id, packet);
        }
    }
}
impl Drop for PartialPacket {
    fn drop(&mut self) {
        if let Some(packet) = &self.packet {
            packet.state.fail(
                ServerFailure::Cleanup("partial setup retired by rollback owner"),
                false,
            );
            packet.state.cancel();
        }
        self.transfer();
    }
}

#[cfg(test)]
thread_local! {
    static SETUP_HOOK: RefCell<Option<Box<dyn FnMut(&str, SetupReceipt)>>> = const { RefCell::new(None) };
    static HEAD_HOOK: RefCell<Option<Box<dyn FnMut(HttpSourceHolder) -> HttpSourceHolder>>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(crate) enum SetupReceipt {
    Watcher(WorkerCleanupObserver),
    Ingress(roxygen_hyper_adapter::body::BodyCleanupObserver),
}
#[cfg(test)]
impl SetupReceipt {
    pub(crate) async fn wait(
        self,
    ) -> Result<(), roxygen_runtime::worker::WorkerCleanupObservationError> {
        match self {
            Self::Watcher(observer) => observer.wait().await,
            Self::Ingress(observer) => observer.wait().await,
        }
    }
}
#[cfg(test)]
pub(crate) fn install_setup_hook(hook: impl FnMut(&str, SetupReceipt) + 'static) {
    SETUP_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}
#[cfg(test)]
pub(crate) fn install_head_hook(hook: impl FnMut(HttpSourceHolder) -> HttpSourceHolder + 'static) {
    HEAD_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}
#[cfg(test)]
fn setup_hook(phase: &str, receipt: SetupReceipt) {
    SETUP_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(phase, receipt);
        }
    });
}

impl Drop for HandlerGuard {
    fn drop(&mut self) {
        if !self.returned {
            self.state.cancel();
        }
    }
}

#[derive(Default)]
struct ResponseControl {
    source: RefCell<Weak<RefCell<ApplicationBodySource>>>,
    stopped: Cell<bool>,
}

impl ResponseControl {
    fn stop(&self) {
        if !self.stopped.replace(true)
            && let Some(source) = self.source.borrow().upgrade()
        {
            source
                .borrow_mut()
                .stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
        }
    }

    fn bind(&self, mut source: ApplicationBodySource) -> ResponseSource {
        if self.stopped.get() {
            source.stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
        }
        let source = Rc::new(RefCell::new(source));
        *self.source.borrow_mut() = Rc::downgrade(&source);
        ResponseSource(source)
    }
}

struct ResponseSource(Rc<RefCell<ApplicationBodySource>>);

impl HttpSource for ResponseSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.0.borrow_mut().read_header_event()
    }
    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.0.borrow_mut().read_body_event(maximum)
    }
    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.0.borrow_mut().stop_reading(error);
    }
    fn set_read_timeout(&mut self, timeout: Duration) {
        self.0.borrow_mut().set_read_timeout(timeout);
    }
}

impl HttpHandler for FacadeHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let Some((id, state)) = self.admission.reserve() else {
            return Box::pin(async { Err(cancelled()) });
        };
        let (request_sender, request) = oneshot::channel();
        let (head, response) = oneshot::channel();
        let (native_sender, native) = oneshot::channel();
        let control = Rc::new(ResponseControl::default());
        let ingress_control: Rc<RefCell<Option<SourceAbortHandle<HttpError>>>> =
            Rc::new(RefCell::new(None));
        let packet = Packet {
            state: Arc::clone(&state),
            lifetime: AppLeafGuard(Arc::clone(&state)),
            envelope: None,
            request,
            head: Some(head),
            preparation: None,
            ingress: None,
            watcher: None,
            native,
            failure: None,
        };
        let mut partial = PartialPacket {
            admission: Arc::clone(&self.admission),
            id,
            packet: Some(packet),
        };
        let mut ingress = None;
        let mut prepared = None;
        let setup = catch_unwind(AssertUnwindSafe(|| {
            let packet = partial.packet.as_mut().expect("owned partial packet");
            packet.envelope = Some(ContextEnvelope::capture());
            let completion = context.exchange_completion().cloned().ok_or_else(|| {
                ServerError::new(
                    ServerFailure::Cleanup("missing native exchange receipt"),
                    false,
                )
            })?;
            let watch_control = Rc::clone(&control);
            let watch_ingress = Rc::clone(&ingress_control);
            let watch_state = Arc::clone(&state);
            let guard = WatchGuard(Arc::clone(&state));
            let registration = self.context.register_scope(async move {
                let _guard = guard;
                let native = completion.wait();
                let app = observe(watch_state.app_leaf.subscribe(), Arc::clone(&watch_state.faults));
                tokio::pin!(native, app);
                let mut native_result = None;
                let mut app_done = false;
                let mut cancelled = false;
                while native_result.is_none() || !app_done {
                    tokio::select! {
                        () = watch_state.cancelled(), if !cancelled => {
                            cancelled = true;
                            watch_state.faults.capture(|| watch_control.stop());
                            if let Some(ingress) = watch_ingress.borrow().as_ref() {
                                watch_state.faults.capture(|| ingress.stop_reading(Some(super::connection::cancelled())));
                            }
                        }
                        result = &mut native, if native_result.is_none() => {
                            native_result = Some(result.map_err(|error| Arc::new(ServerError::new(
                                ServerFailure::ExchangeCleanup(error), false,
                            ))));
                            if let Some(Err(error)) = &native_result {
                                watch_state.faults.record(Err(Arc::clone(error)));
                                watch_state.cancel();
                            }
                        }
                        _ = &mut app, if !app_done => { app_done = true; }
                    }
                }
                watch_state.faults.capture(|| native_sender.send(native_result.expect("observed native leaf")));
            }).map_err(|_| ServerError::new(ServerFailure::Cleanup("exchange watcher registration"), false))?;
            packet.watcher = Some(registration.observe());
            let activated = registration.activate().map_err(|_| {
                ServerError::new(ServerFailure::Cleanup("exchange watcher activation"), false)
            })?;
            #[cfg(test)]
            setup_hook(
                "watcher activation",
                SetupReceipt::Watcher(packet.watcher.as_ref().unwrap().clone()),
            );
            drop(activated);
            let (body, observer) =
                RoxygenBody::prepare_local(&self.context, self.request_body.command_capacity)
                    .map_err(|_| ServerError::new(ServerFailure::Admission, true))?;
            *ingress_control.borrow_mut() = Some(body.abort_handle());
            packet.ingress = Some(observer);
            ingress = Some(body);
            #[cfg(test)]
            setup_hook(
                "ingress prepared",
                SetupReceipt::Ingress(packet.ingress.as_ref().unwrap().clone()),
            );
            let (response, join) = ApplicationBodySource::prepare_response_with_application_join(
                &self.context,
                self.response_body,
            )
            .map_err(|_| ServerError::new(ServerFailure::Admission, true))?;
            packet.preparation = Some(join);
            prepared = Some(response);
            Ok::<(), ServerError>(())
        }));
        match setup {
            Ok(Ok(())) => {}
            Ok(Err(error)) => state.faults.record(Err(Arc::new(error))),
            Err(payload) => state.faults.record(Err(panic_error(payload))),
        }
        if state.faults.result().is_err() {
            state.cancel();
            state.faults.retire(ingress.take());
            state.faults.retire(prepared.take());
            state.faults.retire(source);
            partial.transfer();
            return Box::pin(async { Err(cancelled()) });
        }
        partial.transfer();
        let ingress = ingress.expect("prepared ingress");
        let prepared = prepared.expect("prepared response");
        #[cfg(test)]
        {
            HEAD_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    source = hook(std::mem::take(&mut source));
                }
            });
        }
        let body_options = self.request_body;
        let protocol = self.protocol;
        let rejection = (self.protocol == CodecProtocol::Http11)
            .then_some(self.rejection)
            .flatten();
        let guard = HandlerGuard {
            state: Arc::clone(&state),
            returned: false,
        };
        Box::pin(async move {
            let mut guard = guard;
            let header = tokio::select! {
                result = source.read_header_event() => match result {
                    Ok(header) => header,
                    Err(error) => { state.fail(ServerFailure::Native(error.clone()), true); return Err(error); }
                },
                () = state.cancelled() => return Err(cancelled()),
            };
            let parts = request_head_to_http(header.headers(), NetworkHeadContext::default());
            let rejection = rejection.and_then(|policy| {
                if parts.as_ref().is_ok_and(|parts| {
                    parts.method != Method::CONNECT
                        && !parts.headers.contains_key(http::header::UPGRADE)
                }) {
                    return None;
                }
                rejection_version(header.headers()).map(|version| (policy.status(), version))
            });
            let parts = match parts {
                Ok(parts) => Some(parts),
                Err(error)
                    if rejection.is_some()
                        && matches!(
                            &error,
                            HttpAdapterError::UnsupportedMessageMetadata(
                                UnsupportedMessageMetadata::Upgraded
                                    | UnsupportedMessageMetadata::IngressWebSocketUpgrade
                            )
                        ) =>
                {
                    None
                }
                Err(error) => {
                    state.fail(ServerFailure::Projection(error), true);
                    return Err(cancelled());
                }
            };
            let body = if header.eom() {
                drop(ingress);
                drop(source);
                RoxygenBody::empty()
            } else {
                ingress
                    .into_body(
                        Box::new(source),
                        body_options.maximum_read,
                        body_options.size_hint,
                    )
                    .map_err(|_| cancelled())?
            };
            let trusted = context
                .connection_context()
                .cloned()
                .ok_or_else(cancelled)?;
            let request = if let Some((status, version)) = rejection {
                state.connection.change(Mode::Drain);
                Dispatch::RejectConnectionSwitch {
                    ingress: body,
                    status,
                    version,
                }
            } else {
                let mut request =
                    Request::from_parts(parts.expect("projected service request"), body);
                request.extensions_mut().insert(trusted);
                request.extensions_mut().insert(ServerExchange {
                    state: Arc::clone(&state),
                });
                Dispatch::Service { request, protocol }
            };
            let delivered = state.faults.capture(|| {
                request_sender
                    .send(Ok(request))
                    .map_err(|rejected| state.faults.retire(rejected))
                    .is_ok()
            });
            if delivered != Some(true) {
                return Err(cancelled());
            }
            let head = tokio::select! {
                result = response => result.map_err(|_| cancelled())?,
                () = state.cancelled() => return Err(cancelled()),
            };
            drop(context);
            let response =
                HttpSourceHolder::new(Box::new(control.bind(prepared.into_source(head))));
            guard.returned = true;
            Ok(response)
        })
    }
}

fn cancelled() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::CORO_CANCELLED,
        b"application server exchange cancelled",
    )
}
