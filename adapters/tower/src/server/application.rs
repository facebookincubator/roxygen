use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::poll_fn;
use futures::stream::FuturesUnordered;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Version;
use http::header::CONTENT_LENGTH;
use http::header::TRANSFER_ENCODING;
use http_body::Body;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_http_adapter::project_response_head;
use roxygen_hyper_adapter::body::BodyCleanupObserver;
use roxygen_hyper_adapter::body::BoxError;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyAttachError;
use roxygen_hyper_adapter::source::ApplicationBodyAttachment;
use roxygen_hyper_adapter::source::ApplicationBodyError;
use roxygen_hyper_adapter::source::ApplicationBodyJoinPreparation;
use roxygen_runtime::context::ContextEnvelope;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tower_service::Service;

use super::Admission;
use super::ExchangeState;
use super::Faults;
use super::Mode;
use super::Outcome;
use super::Outcomes;
use super::ServerError;
use super::ServerFailure;
use super::ServerOptions;
use super::SlotId;
use super::connection::Handoff;
use super::connection::bootstrap;
use super::destroy;
use super::observe;
use super::panic_error;
use crate::extensions::RetiringExtensions;

pub(super) enum Dispatch {
    Service {
        request: Request<RoxygenBody>,
        protocol: CodecProtocol,
    },
    RejectConnectionSwitch {
        ingress: RoxygenBody,
        status: StatusCode,
        version: Version,
    },
}

pub(super) struct Packet {
    pub state: Arc<ExchangeState>,
    pub lifetime: AppLeafGuard,
    pub envelope: Option<ContextEnvelope>,
    pub request: oneshot::Receiver<Result<Dispatch, ServerError>>,
    pub head: Option<oneshot::Sender<HttpMessage>>,
    pub preparation: Option<ApplicationBodyJoinPreparation>,
    pub ingress: Option<BodyCleanupObserver>,
    pub watcher: Option<WorkerCleanupObserver>,
    pub native: oneshot::Receiver<Outcome>,
    pub failure: Option<ServerError>,
}

pub(super) struct AppLeafGuard(pub Arc<ExchangeState>);

impl Drop for AppLeafGuard {
    fn drop(&mut self) {
        let missing = self.0.app_leaf.borrow().is_none();
        if missing {
            self.0.lost();
        }
    }
}

type ServiceSlot<S> = Arc<tokio::sync::Mutex<Option<Box<S>>>>;
type ExchangeRun = Pin<Box<dyn Future<Output = (SlotId, Outcome)> + Send>>;

pub(super) struct Channels {
    pub admission: Arc<Admission>,
    pub dispatch: mpsc::Receiver<SlotId>,
    pub request: oneshot::Receiver<()>,
    pub started: oneshot::Sender<Result<Arc<ConnectionContext>, ServerError>>,
}

pub(super) async fn run<Io, S, B>(
    worker: WorkerHandle,
    established: EstablishedIo<Io>,
    service: S,
    options: ServerOptions,
    channels: Channels,
) -> Outcome
where
    Io: AsyncRead + AsyncWrite + Send + 'static,
    S: Service<Request<RoxygenBody>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let Channels {
        admission,
        mut dispatch,
        request,
        started,
    } = channels;
    let signals = Arc::clone(&admission.signals);
    let handoff = Arc::new(Mutex::new(Handoff {
        input: Some(established),
        claimed: false,
        io: None,
        scope: None,
    }));
    let service = Arc::new(tokio::sync::Mutex::new(Some(Box::new(service))));
    let mut outcomes = Outcomes::default();
    let requested = tokio::select! {
        requested = request => requested.is_ok(),
        () = signals.cancelled() => false,
    };
    let startup = if requested && signals.mode() == Mode::Open {
        let native_handoff = Arc::clone(&handoff);
        let native_admission = Arc::clone(&admission);
        match worker
            .call(move |context| bootstrap(context, native_handoff, native_admission, options))
        {
            Ok(call) => call.await.unwrap_or_else(|_| {
                Err(ServerError::new(
                    ServerFailure::Cleanup("bootstrap reply lost"),
                    false,
                ))
            }),
            Err(_) => Err(ServerError::new(ServerFailure::Admission, true)),
        }
    } else {
        Err(ServerError::new(ServerFailure::Cancelled, true))
    };
    if let Err(error) = &startup {
        outcomes.record(Err(Arc::new(error.clone())));
        signals.faults.record(Err(Arc::new(error.clone())));
        signals.change(Mode::Cancel);
    }
    let startup = startup.map_err(|mut error| {
        error.cleanup_confirmed = false;
        error
    });
    if signals
        .faults
        .capture(|| started.send(startup))
        .is_none_or(|result| result.is_err())
    {
        signals.change(Mode::Cancel);
    }
    let (input, claimed, io, scope) = {
        let mut handoff = handoff.lock().unwrap_or_else(|error| error.into_inner());
        (
            handoff.input.take(),
            handoff.claimed,
            handoff.io.take(),
            handoff.scope.take(),
        )
    };
    outcomes.record(destroy(input));
    if claimed && io.is_none() {
        outcomes.fail(
            ServerFailure::Cleanup("claimed I/O has no retirement receipt"),
            false,
        );
    }
    if scope.is_none() {
        signals.publish_native(Ok(()));
        signals.change(Mode::Cancel);
    }
    let mut exchanges: FuturesUnordered<ExchangeRun> = FuturesUnordered::new();
    let mut leaves_published = false;
    let mut io_cancelled = false;
    loop {
        if signals.faults.uncertain() {
            signals.change(Mode::Cancel);
        }
        if signals.mode() == Mode::Cancel && !io_cancelled {
            io_cancelled = true;
            if let Some(io) = &io {
                signals.faults.capture(|| io.cancel());
            }
        }
        while dispatch.try_recv().is_ok() {}
        for (id, packet) in admission.take_packets() {
            let service = Arc::clone(&service);
            exchanges.push(Box::pin(
                async move { (id, exchange(packet, service).await) },
            ));
        }
        if signals.mode() != Mode::Open && admission.app_leaves_done() && !leaves_published {
            for result in admission.leaf_results() {
                outcomes.record(result);
            }
            let retired = service.lock().await.take();
            outcomes.record(destroy(retired));
            leaves_published = true;
            outcomes.record(signals.faults.result());
            signals.publish_leaves(outcomes.result());
        }
        if signals.native.borrow().is_some() && exchanges.is_empty() && admission.settled() {
            break;
        }
        tokio::select! {
            result = exchanges.next(), if !exchanges.is_empty() => {
                let (id, result) = result.expect("nonempty exchange set");
                outcomes.record(result.clone());
                admission.finish(id, result);
            }
            _ = dispatch.recv() => {},
            () = signals.wake.notified() => {},
        }
    }
    if !leaves_published {
        for result in admission.leaf_results() {
            outcomes.record(result);
        }
        let retired = service.lock().await.take();
        outcomes.record(destroy(retired));
        outcomes.record(signals.faults.result());
        signals.publish_leaves(outcomes.result());
    }
    outcomes.record(observe(signals.native.subscribe(), Arc::clone(&signals.faults)).await);
    if let Some(scope) = scope
        && let Err(error) = scope.wait().await
    {
        outcomes.fail(ServerFailure::Scope(error), false);
    }
    if let Some(io) = io {
        let retired = io.completion().wait_retired();
        tokio::pin!(retired);
        let result = tokio::select! {
            result = &mut retired => result,
            () = signals.cancelled() => { signals.faults.capture(|| io.cancel()); retired.await }
        };
        if let Err(error) = result {
            outcomes.fail(ServerFailure::Io(error), false);
        }
    }
    outcomes.record(signals.faults.result());
    outcomes.result()
}

async fn exchange<S, B>(mut packet: Packet, service: ServiceSlot<S>) -> Outcome
where
    S: Service<Request<RoxygenBody>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let mut outcomes = Outcomes::default();
    let faults = Arc::clone(&packet.state.faults);
    if let Some(error) = packet.failure.take() {
        faults.record(Err(Arc::new(error)));
    }
    let mut bodies = JoinSet::new();
    let mut cleanup = None;
    let mut attachment = None;
    if let Some(preparation) = packet.preparation.take() {
        let (driver, ready, observer) = preparation.into_parts();
        cleanup = Some(observer);
        let output_faults = Arc::clone(&faults);
        bodies.spawn(ProtectedFuture::new(
            driver,
            Arc::clone(&faults),
            move |result| {
                if let Err(error) = result {
                    output_faults.fail(ServerFailure::Body(error), true);
                }
            },
        ));
        let ready = tokio::select! {
            result = ready => result,
            () = packet.state.cancelled() => Err(ApplicationBodyAttachError::Closed),
        };
        match ready {
            Ok(ready) => attachment = Some(ready),
            Err(_) => faults.fail(ServerFailure::Cancelled, true),
        }
    }
    let mut services = JoinSet::new();
    let output = Arc::new(Mutex::new(None));
    let mut rejection = None;
    if faults.result().is_ok() && !packet.state.is_cancelled() {
        let request = tokio::select! {
            result = &mut packet.request => result.unwrap_or_else(|_| Err(ServerError::new(ServerFailure::Cancelled, true))),
            () = packet.state.cancelled() => Err(ServerError::new(ServerFailure::Cancelled, true)),
        };
        match request {
            Ok(Dispatch::Service { request, protocol }) => {
                let response_length = if request.method() == Method::HEAD {
                    ResponseLength::Head
                } else if protocol != CodecProtocol::Http2 {
                    ResponseLength::Preserve
                } else if request.method() == Method::CONNECT {
                    ResponseLength::ConnectHttp2
                } else {
                    ResponseLength::InferHttp2
                };
                let service = Arc::clone(&service);
                let child_faults = Arc::clone(&faults);
                let ready_output = Arc::clone(&output);
                let child = async move {
                    let future = {
                        let mut service = service.lock().await;
                        let service = service
                            .as_mut()
                            .expect("service retained through app leaves");
                        if let Err(error) = poll_fn(|context| service.poll_ready(context)).await {
                            child_faults.fail(ServerFailure::Service(error.into()), true);
                            return;
                        }
                        service.call(request)
                    };
                    let sink_faults = Arc::clone(&child_faults);
                    ProtectedFuture::new(future, child_faults, move |result| match result {
                        Ok(response) => {
                            let response = GuardedResponse::new(
                                response,
                                response_length,
                                Arc::clone(&sink_faults),
                            );
                            let old = ready_output
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .replace(response);
                            sink_faults.retire(old);
                        }
                        Err(error) => sink_faults.fail(ServerFailure::Service(error.into()), true),
                    })
                    .await;
                };
                let envelope = packet
                    .envelope
                    .as_ref()
                    .and_then(|envelope| faults.capture(|| envelope.clone()));
                if let Some(envelope) = envelope {
                    let context_faults = Arc::clone(&faults);
                    let contextual = async move {
                        ProtectedFuture::new(envelope.wrap(child), context_faults, |_| {}).await;
                    };
                    services.spawn(ProtectedFuture::new(
                        contextual,
                        Arc::clone(&faults),
                        |_| {},
                    ));
                } else {
                    faults.fail(
                        ServerFailure::Cleanup("service dispatch context unavailable"),
                        false,
                    );
                    faults.retire(child);
                }
            }
            Ok(Dispatch::RejectConnectionSwitch {
                ingress,
                status,
                version,
            }) => {
                retire_in_context(&mut Some(ingress), packet.envelope.as_ref(), &faults);
                let response = Response::builder()
                    .status(status)
                    .version(version)
                    .header(http::header::CONTENT_LENGTH, "0")
                    .header(http::header::CONNECTION, "close")
                    .body(RoxygenBody::empty())
                    .expect("validated rejection and fixed headers");
                rejection = Some(GuardedResponse::new(
                    response,
                    ResponseLength::Preserve,
                    Arc::clone(&faults),
                ));
            }
            Err(error) => faults.record(Err(Arc::new(error))),
        }
    } else if faults.result().is_ok() {
        faults.fail(ServerFailure::Cancelled, true);
    }
    drop(service);
    if !services.is_empty() {
        let joined = tokio::select! {
            result = services.join_next() => result,
            () = packet.state.cancelled() => { services.abort_all(); services.join_next().await }
        };
        if let Err(error) = joined.expect("one service child") {
            if error.is_panic() {
                faults.record(Err(panic_error(error.into_panic())));
            } else {
                faults.fail(ServerFailure::Task(error), true);
            }
        }
    }
    let response = output
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    if let Some(mut response) = response {
        response.deliver(
            attachment.take(),
            packet.head.take(),
            &packet.state,
            packet.envelope.as_ref(),
        );
        response.retire_unattached(packet.envelope.as_ref());
        drop(response);
    }
    if let Some(mut response) = rejection {
        response.deliver(
            attachment.take(),
            packet.head.take(),
            &packet.state,
            packet.envelope.as_ref(),
        );
        response.retire_unattached(packet.envelope.as_ref());
        drop(response);
    }
    if faults.result().is_err() {
        packet.state.cancel();
    }
    faults.retire(attachment.take());
    faults.retire(packet.head.take());
    faults.retire(packet.request);
    faults.retire(packet.envelope.take());
    if faults.uncertain() {
        packet.state.connection.change(Mode::Cancel);
    }
    while let Some(joined) = bodies.join_next().await {
        if let Err(error) = joined {
            if error.is_panic() {
                faults.record(Err(panic_error(error.into_panic())));
            } else {
                faults.fail(ServerFailure::Task(error), true);
            }
        }
    }
    if faults.uncertain() {
        packet.state.connection.change(Mode::Cancel);
    }
    packet.state.publish_leaf(faults.result());
    drop(packet.lifetime);
    let native = packet.native.await.unwrap_or_else(|_| {
        Err(Arc::new(ServerError::new(
            ServerFailure::Cleanup("native exchange watcher lost"),
            false,
        )))
    });
    outcomes.record(native);
    if let Some(watcher) = packet.watcher {
        if let Err(error) = watcher.wait().await {
            outcomes.fail(ServerFailure::Scope(error), false);
        }
    } else {
        outcomes.fail(ServerFailure::Cleanup("missing exchange watcher"), false);
    }
    if let Some(ingress) = packet.ingress
        && let Err(error) = ingress.wait().await
    {
        outcomes.fail(ServerFailure::RequestCleanup(error), false);
    }
    if let Some(cleanup) = cleanup
        && let Err(error) = cleanup.wait().await
    {
        outcomes.fail(ServerFailure::ResponseCleanup(error), false);
    }
    outcomes.record(faults.result());
    outcomes.result()
}

struct ProtectedFuture<Work: Future> {
    future: Option<Pin<Box<Work>>>,
    faults: Arc<Faults>,
    consume: Box<dyn FnMut(Work::Output) + Send>,
}

impl<Work: Future> ProtectedFuture<Work> {
    fn new(
        future: Work,
        faults: Arc<Faults>,
        consume: impl FnMut(Work::Output) + Send + 'static,
    ) -> Self {
        Self {
            future: Some(Box::pin(future)),
            faults,
            consume: Box::new(consume),
        }
    }
    fn retire(&mut self) {
        self.faults.retire(self.future.take());
    }
}
impl<Work: Future> Future for ProtectedFuture<Work> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.future
                .as_mut()
                .expect("unfinished protected task")
                .as_mut()
                .poll(context)
        }));
        match result {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => {
                let faults = Arc::clone(&self.faults);
                faults.capture(|| (self.consume)(output));
            }
            Err(payload) => self.faults.record(Err(panic_error(payload))),
        }
        self.retire();
        Poll::Ready(())
    }
}
impl<Work: Future> Drop for ProtectedFuture<Work> {
    fn drop(&mut self) {
        self.retire();
    }
}

struct GuardedBody<B> {
    body: Option<Pin<Box<B>>>,
    faults: Arc<Faults>,
}
impl<B> Drop for GuardedBody<B> {
    fn drop(&mut self) {
        self.faults.retire(self.body.take());
    }
}
impl<B> Body for GuardedBody<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        if self.body.is_none() {
            return Poll::Ready(None);
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.body
                .as_mut()
                .expect("live body")
                .as_mut()
                .poll_frame(context)
        }));
        let error = match result {
            Ok(Poll::Ready(Some(Err(error)))) => Arc::new(ServerError::new(
                ServerFailure::Body(ApplicationBodyError::Application(error.into())),
                true,
            )),
            Ok(result) => return result.map(|frame| frame.map(|frame| frame.map_err(Into::into))),
            Err(payload) => panic_error(payload),
        };
        self.faults.record(Err(Arc::clone(&error)));
        Poll::Ready(Some(Err(Box::new((*error).clone()))))
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_none() || self.inspect(|body| body.is_end_stream())
    }
    fn size_hint(&self) -> http_body::SizeHint {
        if self.body.is_none() {
            http_body::SizeHint::with_exact(0)
        } else {
            self.inspect(|body| body.size_hint())
        }
    }
}
impl<B> GuardedBody<B> {
    fn inspect<T>(&self, operation: impl FnOnce(&B) -> T) -> T {
        match catch_unwind(AssertUnwindSafe(|| {
            operation(self.body.as_ref().expect("live body").as_ref().get_ref())
        })) {
            Ok(value) => value,
            Err(payload) => {
                self.faults.record(Err(panic_error(payload)));
                std::panic::resume_unwind(Box::new("guarded body inspection failed"));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ResponseLength {
    Preserve,
    Head,
    InferHttp2,
    ConnectHttp2,
}

struct GuardedResponse<B> {
    parts: Option<http::response::Parts>,
    extensions: Option<RetiringExtensions>,
    body: Option<GuardedBody<B>>,
    response_length: ResponseLength,
    faults: Arc<Faults>,
}
impl<B> GuardedResponse<B> {
    fn new(response: Response<B>, response_length: ResponseLength, faults: Arc<Faults>) -> Self {
        let (mut parts, body) = response.into_parts();
        let extensions = RetiringExtensions(std::mem::take(&mut parts.extensions));
        Self {
            parts: Some(parts),
            extensions: Some(extensions),
            body: Some(GuardedBody {
                body: Some(Box::pin(body)),
                faults: Arc::clone(&faults),
            }),
            response_length,
            faults,
        }
    }
    fn retire_unattached(&mut self, envelope: Option<&ContextEnvelope>) {
        retire_in_context(&mut self.extensions, envelope, &self.faults);
        retire_in_context(&mut self.body, envelope, &self.faults);
    }
}

fn retire_in_context<T>(
    value: &mut Option<T>,
    envelope: Option<&ContextEnvelope>,
    faults: &Faults,
) {
    if value.is_some()
        && let Some(envelope) = envelope
    {
        faults.capture(|| envelope.run(|| faults.retire(value.take())));
    }
    faults.retire(value.take());
}
impl<B> Drop for GuardedResponse<B> {
    fn drop(&mut self) {
        self.faults.retire(self.extensions.take());
        self.faults.retire(self.parts.take());
        self.faults.retire(self.body.take());
    }
}
impl<B> GuardedResponse<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    fn deliver(
        &mut self,
        attachment: Option<ApplicationBodyAttachment>,
        head: Option<oneshot::Sender<HttpMessage>>,
        state: &ExchangeState,
        envelope: Option<&ContextEnvelope>,
    ) {
        let parts = self.parts.as_mut().expect("ready response head");
        if matches!(self.response_length, ResponseLength::Head)
            || parts.status.is_informational()
            || matches!(
                parts.status,
                StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED
            )
        {
            parts.headers.remove(TRANSFER_ENCODING);
            if parts.status.is_informational() || parts.status == StatusCode::NO_CONTENT {
                parts.headers.remove(CONTENT_LENGTH);
            }
            retire_in_context(
                &mut self.body.as_mut().expect("unattached response body").body,
                envelope,
                &self.faults,
            );
        }
        if !matches!(
            self.response_length,
            ResponseLength::Preserve | ResponseLength::Head
        ) {
            self.faults.capture(|| {
                let parts = self.parts.as_mut().expect("ready response head");
                if parts.headers.contains_key(CONTENT_LENGTH)
                    || parts.headers.contains_key(TRANSFER_ENCODING)
                    || (matches!(self.response_length, ResponseLength::ConnectHttp2)
                        && parts.status.is_success())
                    || parts.status.is_informational()
                    || matches!(
                        parts.status,
                        StatusCode::NO_CONTENT
                            | StatusCode::RESET_CONTENT
                            | StatusCode::NOT_MODIFIED
                    )
                {
                    return;
                }
                envelope
                    .expect("response retains its dispatch context")
                    .run(|| {
                        let body = self.body.as_ref().expect("unattached response body");
                        if !body.is_end_stream()
                            && let Some(length) = body.size_hint().exact()
                        {
                            parts
                                .headers
                                .insert(CONTENT_LENGTH, HeaderValue::from(length));
                        }
                    });
            });
        }
        let projected = self
            .faults
            .capture(|| project_response_head(self.parts.take().expect("ready response head")));
        let message = match projected {
            Some(Ok((message, sidecar))) => {
                self.faults.retire(sidecar);
                Some(message)
            }
            Some(Err(error)) => {
                self.faults.fail(ServerFailure::Projection(error), true);
                None
            }
            None => None,
        };
        retire_in_context(&mut self.extensions, envelope, &self.faults);
        if self.faults.result().is_err() || state.is_cancelled() {
            self.faults.retire(attachment);
            self.faults.retire(head);
            return;
        }
        if let (Some(attachment), Some(head), Some(message)) = (attachment, head, message) {
            let mut attached = None;
            let faults = Arc::clone(&self.faults);
            let entered = faults.capture(|| {
                envelope
                    .expect("response retains its dispatch context")
                    .run(|| {
                        let body = self.body.take().expect("unattached response body");
                        attached = Some(attachment.attach(body));
                    });
            });
            match attached {
                Some(Ok(())) => {
                    if entered.is_none()
                        || self.faults.result().is_err()
                        || self.faults.capture(|| {
                            head.send(message)
                                .map_err(|rejected| self.faults.retire(rejected))
                                .is_ok()
                        }) != Some(true)
                    {
                        state.cancel();
                    }
                }
                Some(Err(rejected)) => {
                    self.body = Some(rejected.body);
                    state.cancel();
                }
                None => state.cancel(),
            }
        } else {
            state.cancel();
        }
    }
}
