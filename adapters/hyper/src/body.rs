/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

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

/// The boxed error type that hyper body consumers expect.
///
/// Source errors can be recovered with `downcast_ref`.
pub type BoxError = Box<dyn Error + Send + Sync>;

/// Moves `bytes` into a bounded [`BufQueue`] without copying, sharing their
/// backing allocation.
pub fn admit_owned_bytes(bytes: Bytes, limit: usize) -> Result<BufQueue, BufQueueError> {
    BufQueue::from_bytes(bytes, limit)
}

/// Copies `bytes` into a bounded [`BufQueue`].
///
/// Use this when `bytes` may be a view into a larger allocation. The copy is
/// sized to the visible range.
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

/// Observes when a body's source has been released on its worker.
///
/// [`wait`](Self::wait) completes once the source scope has retired, and
/// reports whether the source and the native events read from it were dropped
/// cleanly. It does not report whether the body was delivered. Dropping the
/// observer does not cancel the body.
///
/// The result is an error if the scope's cleanup failed or its outcome was
/// lost. It is also an error if dropping a source-owned value, a native event
/// or a registration panicked. A panic in the source's `stop_reading` is an
/// error even if the source was then dropped cleanly. A panic while polling a
/// read does not make the result an error by itself. Read replies that the
/// consumer has accepted belong to the consumer, so dropping them cannot change
/// the result.
#[derive(Clone)]
pub struct BodyCleanupObserver {
    source: SourceCleanupObserver,
    projection: NativeProjectionReporter,
}

impl BodyCleanupObserver {
    /// Waits for the source scope to retire, then returns an error for any of
    /// the failures listed on [`BodyCleanupObserver`]. Repeated calls return the
    /// same result.
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

/// An `http_body::Body` that reads a Roxygen source owned by a worker.
///
/// At most one source read is in flight, and at most one bounded `BufQueue`
/// event waits for delivery. Dropping the body sends a cancellation to the
/// worker, which then calls `stop_reading` on the source and drops it.
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
    /// Bounds on the remaining data length, which must be accurate.
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

/// An owner-local, registered body export that is waiting for its source.
///
/// Create it with [`RoxygenBody::prepare_local`] before an exchange begins.
/// Call [`into_body`](Self::into_body) from the exchange's continuation after
/// reading its head. Dropping an unused preparation cancels its scope without
/// blocking. If the head ends the message, drop this value and use
/// [`RoxygenBody::empty`]. The observer then reports only on this unused scope,
/// not on the exchange or its source.
pub struct PreparedRoxygenBody {
    attachment: LocalSourceAttachment<LocalBodySource>,
    source: BodySource,
    projection: NativeProjectionReporter,
}

impl PreparedRoxygenBody {
    /// Returns a cancellation handle that does not go through the bounded source
    /// queues.
    pub fn abort_handle(&self) -> SourceAbortHandle<HttpError> {
        self.source.abort_handle()
    }

    /// Binds the source after reading a head that does not end the message.
    ///
    /// A preparation can still bind during the worker's shutdown grace period.
    /// After a forced shutdown or cancellation this returns an error, and the
    /// source is dropped on its worker. `maximum_read` and `size_hint` apply to
    /// the body, and `size_hint` must be accurate. A zero size hint does not
    /// suppress trailers. The source need not be `Send`, `Sync` or `Unpin`; the
    /// returned body is `Send`.
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
    /// Creates a body that has already ended.
    ///
    /// Use it when the header event carried EOM. It does not read, stop or hold a
    /// source, and it does not need a worker. The caller still drops its source
    /// on the owner worker. Decide by the event's EOM, not by a zero size hint,
    /// because a source with no remaining data may still produce trailers.
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
    /// The source's scope outlives the worker command that calls this. Dropping
    /// the returned body only requests cancellation. The worker then stops and
    /// drops the source before its scope completes. If the header event ended the
    /// message, use [`Self::empty`] instead of exporting the exhausted source.
    pub fn try_from_local(
        context: &WorkerContext,
        source: Box<dyn HttpSource>,
        options: LocalBodyOptions,
    ) -> Result<Self, RemoteSourceCreateError> {
        Self::try_from_local_with_observer(context, source, options).map(|(body, _observer)| body)
    }

    /// Like [`Self::try_from_local`], and also returns an observer of the
    /// source's cleanup.
    ///
    /// The observer completes when the worker has dropped the source scope and
    /// everything it captured. It does not report whether the body was
    /// delivered, and dropping it does not cancel the body. See
    /// [`BodyCleanupObserver`] for the failures it reports.
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

    /// Registers a body scope before an exchange starts.
    ///
    /// Returns a single-use [`PreparedRoxygenBody`] and a cleanup observer
    /// before the source or head exists. The same scope waits for
    /// [`PreparedRoxygenBody::into_body`] and then drives reads, so no extra task
    /// is spawned. Preparing fails if the worker is not accepting new work, and
    /// the scope holds the worker's graceful shutdown open until it retires. It
    /// does not wait for the exchange or application tasks; join those
    /// separately.
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

    /// Creates a body with the given remaining-data bounds, which must be accurate.
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

    /// Returns a handle that cancels the source. Cancelling twice is harmless.
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
