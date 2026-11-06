use std::any::Any;
use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpBodyEventType;
use roxygen_core::events::ResumeFuture;
use roxygen_core::source::HttpSource;
use roxygen_runtime::source_proxy::LocalBodyRead;
use roxygen_runtime::source_proxy::LocalHttpSource;
use roxygen_runtime::source_proxy::LocalSourceAttachment;
use roxygen_runtime::source_proxy::LocalSourceFuture;
use roxygen_runtime::source_proxy::RemoteHttpSource;
use roxygen_runtime::source_proxy::RemoteSourceCreateError;
use roxygen_runtime::source_proxy::RemoteSourceError;
use roxygen_runtime::source_proxy::RemoteSourceEvent;
use roxygen_runtime::source_proxy::SourceAbortHandle;
use roxygen_runtime::source_proxy::SourceCleanupObserver;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;

/// The error surface expected by Hyper-compatible body consumers.
///
/// Source errors remain available for typed inspection through `downcast_ref`
/// on [`Error`] trait objects.
pub type BoxError = Box<dyn Error + Send + Sync>;

/// Moves adapter-provided bytes into bounded Roxygen storage while preserving
/// their immutable shared backing allocation.
pub fn admit_owned_bytes(bytes: Bytes, limit: usize) -> Result<BufQueue, BufQueueError> {
    BufQueue::from_bytes(bytes, limit)
}

/// Copies adapter-provided bytes into bounded Roxygen storage.
///
/// `Bytes` crossing this public boundary is not assumed to own exactly its
/// visible range. `BufQueue::copy_from_slice` performs the required exact-sized
/// copy before admitting the bytes.
pub fn admit_untrusted_bytes(bytes: Bytes, limit: usize) -> Result<BufQueue, BufQueueError> {
    BufQueue::copy_from_slice(bytes.as_ref(), limit)
}

#[derive(Clone, Copy)]
struct UnusedHeaderEvent;

impl RemoteSourceEvent for UnusedHeaderEvent {
    fn is_terminal(&self) -> bool {
        false
    }
}

enum RemoteBodyEventKind {
    Data(BufQueue),
    Trailers(roxygen_core::headers::HttpHeaders),
    Padding,
    Unsupported(HttpBodyEventType),
}

struct RemoteBodyEvent {
    kind: RemoteBodyEventKind,
    terminal: bool,
}

impl RemoteSourceEvent for RemoteBodyEvent {
    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

struct LocalBodySource {
    source: Box<dyn HttpSource>,
    projection: NativeProjectionReporter,
}

struct NativeBodyRead {
    future: roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    projection: NativeProjectionReporter,
}

impl Future for NativeBodyRead {
    type Output = Result<LocalBodyRead<RemoteBodyEvent>, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(context).map(|result| {
            result.map(|event| NativeEventOwner::new(event, self.projection.clone()).translate())
        })
    }
}

struct NativeBodyResume {
    future: ResumeFuture,
}

impl Future for NativeBodyResume {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(context).map(|_status| ())
    }
}

impl LocalHttpSource for LocalBodySource {
    type HeaderEvent = UnusedHeaderEvent;
    type BodyEvent = RemoteBodyEvent;
    type Error = HttpError;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<Self::HeaderEvent, Self::Error>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"the Hyper body adapter cannot read header events",
        ))))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<Self::BodyEvent>, Self::Error>> {
        Box::pin(NativeBodyRead {
            future: self.source.read_body_event(maximum),
            projection: self.projection.clone(),
        })
    }

    fn stop_reading(&mut self, error: Option<Self::Error>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            self.source.stop_reading(error.map(|error| error.code()));
        })) {
            self.projection
                .destruction_failed
                .store(true, Ordering::Release);
            resume_unwind(payload);
        }
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.source.set_read_timeout(timeout);
    }
}

type NativePanic = Box<dyn Any + Send>;

fn discard_native_panic(payload: NativePanic) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

fn finish_native_disposal(failure: Option<NativePanic>, unwinding: bool) {
    if let Some(payload) = failure {
        if unwinding {
            discard_native_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

#[derive(Clone, Default)]
struct NativeProjectionReporter {
    destruction_failed: Arc<AtomicBool>,
}

impl NativeProjectionReporter {
    fn retain_failure(&self, first: &mut Option<NativePanic>, result: Result<(), NativePanic>) {
        if let Err(payload) = result {
            self.destruction_failed.store(true, Ordering::Release);
            if first.is_none() {
                *first = Some(payload);
            } else {
                discard_native_panic(payload);
            }
        }
    }

    fn dispose<T>(&self, value: T, first: &mut Option<NativePanic>) {
        self.retain_failure(first, catch_unwind(AssertUnwindSafe(|| drop(value))));
    }
}

struct NativeEventOwner {
    kind: Option<HttpBodyEventKind>,
    registrations: Vec<HttpByteEventRegistration>,
    terminal: bool,
    projection: NativeProjectionReporter,
}

impl NativeEventOwner {
    fn new(mut event: HttpBodyEvent, projection: NativeProjectionReporter) -> Self {
        let terminal = event.eom();
        let registrations = event.take_byte_event_registrations();
        Self {
            kind: Some(event.into_kind()),
            registrations,
            terminal,
            projection,
        }
    }

    fn dispose_registrations(&mut self, first: &mut Option<NativePanic>) {
        for registration in std::mem::take(&mut self.registrations) {
            for event in registration.into_event_registrations() {
                self.projection.dispose(event, first);
            }
        }
    }

    fn dispose_kind(&mut self, first: &mut Option<NativePanic>) {
        match self.kind.take() {
            Some(HttpBodyEventKind::PushPromise(mut push)) => {
                let mut holder = push.move_push_source();
                let mut source = holder.as_mut().and_then(|holder| {
                    holder
                        .release()
                        .expect("an unread push holder has no active read")
                });
                if let Some(source) = source.as_mut() {
                    self.projection.retain_failure(
                        first,
                        catch_unwind(AssertUnwindSafe(|| source.stop_reading(None))),
                    );
                }
                self.projection.dispose(source, first);
                self.projection.dispose(holder, first);
                self.projection.dispose(push, first);
            }
            other => self.projection.dispose(other, first),
        }
    }

    fn translate(mut self) -> LocalBodyRead<RemoteBodyEvent> {
        let mut failure = None;
        self.dispose_registrations(&mut failure);
        finish_native_disposal(failure, std::thread::panicking());
        if matches!(self.kind.as_ref(), Some(HttpBodyEventKind::PushPromise(_))) {
            let mut failure = None;
            self.dispose_kind(&mut failure);
            finish_native_disposal(failure, std::thread::panicking());
            return LocalBodyRead::Event(RemoteBodyEvent {
                kind: RemoteBodyEventKind::Unsupported(HttpBodyEventType::PushPromise),
                terminal: self.terminal,
            });
        }
        let kind = match self.kind.take().expect("a native event retains its kind") {
            HttpBodyEventKind::Body(body) => RemoteBodyEventKind::Data(body),
            HttpBodyEventKind::Trailers(trailers) => RemoteBodyEventKind::Trailers(trailers),
            HttpBodyEventKind::Padding(_) => RemoteBodyEventKind::Padding,
            HttpBodyEventKind::Suspend(resume) => {
                return LocalBodyRead::Suspend(Box::pin(NativeBodyResume { future: resume }));
            }
            HttpBodyEventKind::Upgrade => {
                RemoteBodyEventKind::Unsupported(HttpBodyEventType::Upgrade)
            }
            HttpBodyEventKind::PushPromise(_) => {
                unreachable!("push disposal is handled separately")
            }
        };
        LocalBodyRead::Event(RemoteBodyEvent {
            kind,
            terminal: self.terminal,
        })
    }
}

impl Drop for NativeEventOwner {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = None;
        self.dispose_registrations(&mut failure);
        self.dispose_kind(&mut failure);
        finish_native_disposal(failure, unwinding);
    }
}

/// Observation of actual body-source scope retirement and native projection disposal.
///
/// This owns no task and does not confirm successful HTTP delivery, child joins,
/// worker joins, or I/O completion. Dropping it does not cancel the body. Failed
/// native event/registration disposal remains unconfirmed. Interrupted native
/// `stop_reading` retirement is conservatively unconfirmed, including a callback
/// panic followed by clean destruction. An ordinary read-poll panic with clean
/// retirement does not itself make the receipt unconfirmed. Consumer-owned
/// replies cannot change the outcome after the source scope retires.
#[derive(Clone)]
pub struct BodyCleanupObserver {
    source: SourceCleanupObserver,
    projection: NativeProjectionReporter,
}

impl BodyCleanupObserver {
    /// Awaits actual source-scope retirement before reporting known projection
    /// disposal failures or interrupted native stop retirement. These failures
    /// and publisher loss are sticky.
    pub fn wait(
        &self,
    ) -> impl Future<Output = Result<(), WorkerCleanupObservationError>> + Send + 'static {
        let source = self.source.clone();
        let failed = Arc::clone(&self.projection.destruction_failed);
        async move {
            source.wait().await?;
            if failed.load(Ordering::Acquire) {
                Err(WorkerCleanupObservationError::Unconfirmed)
            } else {
                Ok(())
            }
        }
    }
}

type BodySource = RemoteHttpSource<UnusedHeaderEvent, RemoteBodyEvent, HttpError>;
type BodyReadResult = Result<RemoteBodyEvent, RemoteSourceError<HttpError>>;
type BodyReadFuture = Pin<Box<dyn Future<Output = (BodySource, BodyReadResult)> + Send>>;

/// A bounded `http_body::Body` facade over one worker-owned Roxygen source.
///
/// The adapter keeps at most one source read in flight and one bounded
/// `BufQueue` event awaiting delivery. Dropping it requests owner-worker
/// cancellation; the worker bridge performs `stop_reading` and destruction.
pub struct RoxygenBody {
    source: Option<BodySource>,
    read: Option<BodyReadFuture>,
    abort: SourceAbortHandle<HttpError>,
    pending_data: VecDeque<Bytes>,
    terminal_after_data: bool,
    terminal: bool,
    maximum_read: NonZeroU32,
    size_hint: SizeHint,
}

/// Queue, read, and initial size bounds for exporting an existing local source.
#[derive(Clone, Copy, Debug)]
pub struct LocalBodyOptions {
    /// Maximum number of queued source commands and controls.
    pub command_capacity: usize,
    /// Maximum logical data bytes requested per source event.
    pub maximum_read: NonZeroU32,
    /// Truthful remaining-data bounds supplied by the source owner.
    pub size_hint: SizeHint,
}

impl Default for LocalBodyOptions {
    fn default() -> Self {
        Self {
            command_capacity: 1,
            maximum_read: NonZeroU32::new(64 * 1024).expect("a fixed read bound is nonzero"),
            size_hint: SizeHint::new(),
        }
    }
}

/// An owner-local, already registered body export awaiting the actual source.
///
/// Prepare before an exchange begins. Bind only after consuming its real native
/// head, using that exchange's already admitted continuation. Dropping an unused
/// preparation cancels its scope without blocking or joining. For header EOM,
/// drop this reservation and return [`RoxygenBody::empty`]; its receipt still
/// describes only this unused scope, not the native exchange or source.
pub struct PreparedRoxygenBody {
    attachment: LocalSourceAttachment<LocalBodySource>,
    source: BodySource,
    projection: NativeProjectionReporter,
}

impl PreparedRoxygenBody {
    /// Returns cancellation independent of the bounded source data queues.
    pub fn abort_handle(&self) -> SourceAbortHandle<HttpError> {
        self.source.abort_handle()
    }

    /// Binds the remaining native source after its nonterminal head is known.
    ///
    /// Previously admitted preparation may bind during grace, but forced
    /// shutdown or cancellation rejects and retires the input on its owner.
    /// Read bounds and truthful hints apply to the real body. A zero size hint
    /// does not suppress trailers. No `Send` factory or `Sync`/`Unpin` source is
    /// required; the resulting body is `Send`.
    pub fn into_body(
        self,
        source: Box<dyn HttpSource>,
        maximum_read: NonZeroU32,
        size_hint: SizeHint,
    ) -> Result<RoxygenBody, RemoteSourceCreateError> {
        let Self {
            attachment,
            source: remote,
            projection,
        } = self;
        attachment.attach(LocalBodySource { source, projection })?;
        Ok(RoxygenBody::from_remote(remote, maximum_read, size_hint))
    }
}

impl RoxygenBody {
    /// Creates an already-terminal body, including when the consumed native
    /// header event carried EOM. No source is read, stopped, or retained, and no
    /// worker admission is required. The caller retires its source on the owner.
    ///
    /// Use the native event's EOM, not a zero data-size hint: a nonterminal source
    /// with zero remaining data may still produce trailers.
    pub fn empty() -> Self {
        Self {
            source: None,
            read: None,
            abort: SourceAbortHandle::completed(),
            pending_data: VecDeque::new(),
            terminal_after_data: false,
            terminal: true,
            maximum_read: LocalBodyOptions::default().maximum_read,
            size_hint: SizeHint::with_exact(0),
        }
    }

    /// Exports a source already created on this worker, without a `Send` factory.
    ///
    /// Scope ownership survives the initializing command. Dropping the returned
    /// Send body only requests cancellation; the worker stops and destroys the
    /// local source before its scope completes.
    /// After consuming a terminal native header event, use [`Self::empty`]
    /// instead of exporting the exhausted source.
    pub fn try_from_local(
        context: &WorkerContext,
        source: Box<dyn HttpSource>,
        options: LocalBodyOptions,
    ) -> Result<Self, RemoteSourceCreateError> {
        Self::try_from_local_with_observer(context, source, options).map(|(body, _observer)| body)
    }

    /// Exports an owner-local source with its actual bridge-scope receipt.
    ///
    /// The observer confirms registered future/capture retirement, not body
    /// success, frame delivery, native exchange retirement, child/worker joins,
    /// or I/O completion. Dropping the observer never cancels the body. A scope
    /// destructor failure or publisher loss remains unconfirmed.
    /// Known source-owned future and output destruction failures, and interrupted
    /// native stop retirement, also remain unconfirmed. Once a read reply is
    /// accepted for this body, its consumer owns that reply; later consumer
    /// destruction cannot change the receipt.
    pub fn try_from_local_with_observer(
        context: &WorkerContext,
        source: Box<dyn HttpSource>,
        options: LocalBodyOptions,
    ) -> Result<(Self, BodyCleanupObserver), RemoteSourceCreateError> {
        let projection = NativeProjectionReporter::default();
        let (source, observer) = RemoteHttpSource::try_from_local_with_observer(
            context,
            options.command_capacity,
            LocalBodySource {
                source,
                projection: projection.clone(),
            },
        )?;
        Ok((
            Self::from_remote(source, options.maximum_read, options.size_hint),
            BodyCleanupObserver {
                source: observer,
                projection,
            },
        ))
    }

    /// Registers a body-driving scope before an exchange starts.
    ///
    /// Returns its owner-local, single-use handoff and actual retirement
    /// observer before any source or native head is available. The same scope
    /// awaits binding and drives reads; no watcher or additional task is spawned.
    /// Preparation requires open admission and retains grace credit until scope
    /// retirement. Keep native exchange/application joins independently owned.
    pub fn prepare_local(
        context: &WorkerContext,
        command_capacity: usize,
    ) -> Result<(PreparedRoxygenBody, BodyCleanupObserver), RemoteSourceCreateError> {
        let projection = NativeProjectionReporter::default();
        let (attachment, source, observer) =
            RemoteHttpSource::prepare_local::<LocalBodySource>(context, command_capacity)?;
        Ok((
            PreparedRoxygenBody {
                attachment,
                source,
                projection: projection.clone(),
            },
            BodyCleanupObserver {
                source: observer,
                projection,
            },
        ))
    }

    /// Creates a body with unknown remaining data length.
    pub fn new<F>(
        worker: &WorkerHandle,
        command_capacity: usize,
        maximum_read: NonZeroU32,
        source_factory: F,
    ) -> Result<Self, RemoteSourceCreateError>
    where
        F: FnOnce(&WorkerContext) -> Box<dyn HttpSource> + Send + 'static,
    {
        Self::with_size_hint(
            worker,
            command_capacity,
            maximum_read,
            SizeHint::new(),
            source_factory,
        )
    }

    /// Creates a body with caller-supplied truthful remaining-data bounds.
    pub fn with_size_hint<F>(
        worker: &WorkerHandle,
        command_capacity: usize,
        maximum_read: NonZeroU32,
        size_hint: SizeHint,
        source_factory: F,
    ) -> Result<Self, RemoteSourceCreateError>
    where
        F: FnOnce(&WorkerContext) -> Box<dyn HttpSource> + Send + 'static,
    {
        let source =
            RemoteHttpSource::new(worker, command_capacity, move |context| LocalBodySource {
                source: source_factory(context),
                projection: NativeProjectionReporter::default(),
            })?;
        Ok(Self::from_remote(source, maximum_read, size_hint))
    }

    fn from_remote(source: BodySource, maximum_read: NonZeroU32, size_hint: SizeHint) -> Self {
        let abort = source.abort_handle();
        Self {
            source: Some(source),
            read: None,
            abort,
            pending_data: VecDeque::new(),
            terminal_after_data: false,
            terminal: false,
            maximum_read,
            size_hint,
        }
    }

    /// Returns a typed, idempotent cancellation capability for this source.
    pub fn abort_handle(&self) -> SourceAbortHandle<HttpError> {
        self.abort.clone()
    }

    fn start_read(&mut self) {
        let mut source = self
            .source
            .take()
            .expect("a nonterminal idle body retains its remote source");
        let maximum = self.maximum_read.get();
        self.read = Some(Box::pin(async move {
            let result = source.read_body_event(maximum).await;
            (source, result)
        }));
    }

    fn emit_pending_data(&mut self) -> Option<Frame<Bytes>> {
        let data = self.pending_data.pop_front()?;
        self.consume_hint(data.len());
        if self.pending_data.is_empty() && self.terminal_after_data {
            self.finish();
        }
        Some(Frame::data(data))
    }

    fn queue_data(&mut self, body: BufQueue, terminal: bool) {
        self.pending_data = body.into_bytes_segments();
        if self.pending_data.is_empty() {
            self.pending_data.push_back(Bytes::new());
        }
        self.terminal_after_data = terminal;
    }

    fn consume_hint(&mut self, amount: usize) {
        let amount = amount as u64;
        let lower = self.size_hint.lower().saturating_sub(amount);
        let upper = self
            .size_hint
            .upper()
            .map(|upper| upper.saturating_sub(amount));
        let mut remaining = SizeHint::new();
        remaining.set_lower(lower);
        if let Some(upper) = upper {
            remaining.set_upper(upper);
        }
        self.size_hint = remaining;
    }

    fn finish(&mut self) {
        self.terminal = true;
        self.terminal_after_data = false;
        self.source = None;
        self.read = None;
        self.size_hint = SizeHint::with_exact(0);
    }

    fn unsupported_event_error(event_type: HttpBodyEventType) -> BoxError {
        let message: &[u8] = match event_type {
            HttpBodyEventType::Upgrade => {
                b"an upgrade event cannot be represented by http_body::Frame"
            }
            HttpBodyEventType::PushPromise => {
                b"a push-promise event cannot be represented by http_body::Frame"
            }
            _ => b"the source returned an unsupported body event",
        };
        Box::new(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            message,
        ))
    }
}

impl Body for RoxygenBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        if let Some(frame) = self.emit_pending_data() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if self.read.is_none() {
            self.start_read();
        }

        let Poll::Ready((source, result)) = self
            .read
            .as_mut()
            .expect("a started body read retains its future")
            .as_mut()
            .poll(context)
        else {
            return Poll::Pending;
        };
        self.read = None;
        self.source = Some(source);

        let event = match result {
            Ok(event) => event,
            Err(error) => {
                self.finish();
                return Poll::Ready(Some(Err(Box::new(error))));
            }
        };
        match event.kind {
            RemoteBodyEventKind::Data(body) => {
                self.queue_data(body, event.terminal);
                Poll::Ready(Some(Ok(self
                    .emit_pending_data()
                    .expect("a queued body event produces one data frame"))))
            }
            RemoteBodyEventKind::Trailers(trailers) => {
                let trailers = match trailers_to_http(trailers) {
                    Ok(trailers) => trailers,
                    Err(error) => {
                        self.finish();
                        return Poll::Ready(Some(Err(error)));
                    }
                };
                self.finish();
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            RemoteBodyEventKind::Padding => {
                if event.terminal {
                    self.finish();
                    Poll::Ready(None)
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            RemoteBodyEventKind::Unsupported(event_type) => {
                let error = Self::unsupported_event_error(event_type);
                self.finish();
                Poll::Ready(Some(Err(error)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.terminal
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }
}

impl Drop for RoxygenBody {
    fn drop(&mut self) {
        let _accepted = self.abort.stop_reading(None);
    }
}

fn trailers_to_http(trailers: roxygen_core::headers::HttpHeaders) -> Result<HeaderMap, BoxError> {
    let mut converted = HeaderMap::new();
    for (_, name, value) in trailers.iter() {
        let name = HeaderName::from_bytes(name).map_err(|error| Box::new(error) as BoxError)?;
        let value = HeaderValue::from_bytes(value).map_err(|error| Box::new(error) as BoxError)?;
        converted
            .try_append(name, value)
            .map_err(|error| Box::new(error) as BoxError)?;
    }
    Ok(converted)
}
