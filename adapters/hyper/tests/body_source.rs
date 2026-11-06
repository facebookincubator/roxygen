use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomPinned;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http::HeaderValue;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use http_body_util::BodyExt;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_runtime::context::RuntimeContext;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::source_proxy::RemoteSourceError;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::Barrier;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::task::LocalSet;

use crate::body::LocalBodyOptions;
use crate::body::RoxygenBody;
use crate::source::ApplicationBodyAttachError;
use crate::source::ApplicationBodyAttachment;
use crate::source::ApplicationBodyAttachmentReady;
use crate::source::ApplicationBodyCleanupObserver;
use crate::source::ApplicationBodyCompletion;
use crate::source::ApplicationBodyError;
use crate::source::ApplicationBodyJoinDriver;
use crate::source::ApplicationBodyJoinPreparation;
use crate::source::ApplicationBodyOptions;
use crate::source::ApplicationBodySource;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
#[error("original application failure")]
struct OriginalError(Arc<()>);

#[derive(Clone, Debug, PartialEq)]
enum Action {
    Poll(ThreadId),
    Hint(ThreadId),
    End(ThreadId),
    Drop(ThreadId),
}

type Actions = Arc<Mutex<Vec<Action>>>;

struct TrackedBody {
    frames: RefCell<VecDeque<Result<Frame<Bytes>, OriginalError>>>,
    actions: Actions,
    owner: ThreadId,
    context: Option<RuntimeContext>,
    pending: bool,
    polled: RefCell<Option<oneshot::Sender<()>>>,
    ready: RefCell<Option<oneshot::Receiver<()>>>,
    hint: SizeHint,
    panic_on_poll: bool,
    dropped: Option<oneshot::Sender<()>>,
    _pin: PhantomPinned,
}

impl TrackedBody {
    fn new(frames: Vec<Result<Frame<Bytes>, OriginalError>>, actions: &Actions) -> Self {
        Self {
            frames: RefCell::new(frames.into()),
            actions: Arc::clone(actions),
            owner: std::thread::current().id(),
            context: RuntimeContext::current(),
            pending: false,
            polled: RefCell::new(None),
            ready: RefCell::new(None),
            hint: SizeHint::new(),
            panic_on_poll: false,
            dropped: None,
            _pin: PhantomPinned,
        }
    }

    fn record(&self, action: Action) {
        assert_eq!(self.owner, std::thread::current().id());
        assert!(
            Handle::try_current().is_ok(),
            "body operations stay in the app runtime"
        );
        assert_eq!(RuntimeContext::current(), self.context);
        self.actions.lock().unwrap().push(action);
    }
}

impl Body for TrackedBody {
    type Data = Bytes;
    type Error = OriginalError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, OriginalError>>> {
        let this = self.as_ref().get_ref();
        this.record(Action::Poll(std::thread::current().id()));
        if let Some(polled) = this.polled.borrow_mut().take() {
            let _ = polled.send(());
        }
        if let Some(ready) = this.ready.borrow_mut().as_mut()
            && Pin::new(ready).poll(context).is_pending()
        {
            return Poll::Pending;
        }
        this.ready.borrow_mut().take();
        assert!(!this.panic_on_poll, "deliberate producer panic");
        match this.frames.borrow_mut().pop_front() {
            Some(frame) => Poll::Ready(Some(frame)),
            None if this.pending => Poll::Pending,
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.record(Action::Hint(std::thread::current().id()));
        self.hint
    }

    fn is_end_stream(&self) -> bool {
        self.record(Action::End(std::thread::current().id()));
        self.frames.borrow().is_empty() && !self.pending
    }
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        self.record(Action::Drop(std::thread::current().id()));
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

fn run_case<Work: Future<Output = ()>>(case: impl FnOnce(WorkerHandle, RuntimeHandle) -> Work) {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .unwrap();
    let handle = owner.handle();
    let worker = handle.worker(0).unwrap();
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    app.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, case(worker, handle.clone()))
            .await
            .expect("body scenario bounded");
        tokio::time::timeout(TEST_TIMEOUT, handle.quiesce(Duration::ZERO))
            .await
            .expect("scope cleanup bounded")
            .expect("all app tasks joined before quiescence");
    });
    owner
        .join()
        .expect("worker physically joined outside Tokio");
}

fn message() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(200).unwrap();
    message
}

async fn export(
    worker: &WorkerHandle,
    options: ApplicationBodyOptions,
) -> (
    RoxygenBody,
    ApplicationBodyAttachment,
    ApplicationBodyCompletion,
) {
    worker
        .call(move |context| {
            let (source, attachment, completion) =
                ApplicationBodySource::prepare(context, message(), options).unwrap();
            let body =
                RoxygenBody::try_from_local(context, Box::new(source), LocalBodyOptions::default())
                    .unwrap();
            (body, attachment, completion)
        })
        .expect("preparation admitted")
        .await
        .expect("initialization finishes before attachment")
}

fn assert_dropped(actions: &Actions) {
    assert_eq!(
        actions
            .lock()
            .unwrap()
            .iter()
            .filter(|action| matches!(action, Action::Drop(_)))
            .count(),
        1
    );
}

#[test]
fn application_runtime_body() {
    fn assert_send<Value: Send>() {}
    assert_send::<ApplicationBodyAttachment>();
    assert_send::<ApplicationBodyCompletion>();
    run_case(|worker, _handle| async move {
        let context = RuntimeContext::new();
        let _context = context.enter();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let bytes = Bytes::from(vec![b'x'; 16]).slice(3..11);
        let pointer = bytes.as_ptr();
        let mut trailers = HeaderMap::new();
        trailers.append("x-result", HeaderValue::from_static("first"));
        trailers.append("x-result", HeaderValue::from_static("second"));
        let mut app_body = TrackedBody::new(
            vec![Ok(Frame::data(bytes)), Ok(Frame::trailers(trailers))],
            &actions,
        );
        app_body.hint = SizeHint::with_exact(8);
        let (mut body, attachment, completion) = export(
            &worker,
            ApplicationBodyOptions {
                maximum_demand_bytes: NonZeroU32::new(3).unwrap(),
                ..ApplicationBodyOptions::default()
            },
        )
        .await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("attachment rejected: {:?}", error.reason));
        let mut offset = 0;
        for expected in [3, 3, 2] {
            let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(data.len(), expected);
            assert_eq!(data.as_ptr(), pointer.wrapping_add(offset));
            offset += expected;
        }
        let trailers = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_trailers()
            .unwrap();
        assert_eq!(
            trailers
                .get_all("x-result")
                .iter()
                .map(|value| value.as_bytes())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
        completion
            .await
            .expect("application task physically joined");
        assert_dropped(&actions);
        assert_eq!(
            actions
                .lock()
                .unwrap()
                .iter()
                .filter(|action| matches!(action, Action::Poll(_)))
                .count(),
            2
        );
    });
}

#[test]
fn original_application_error_and_task_panic_survive_completion() {
    run_case(|worker, _handle| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let identity = Arc::new(());
        let app_body = TrackedBody::new(vec![Err(OriginalError(Arc::clone(&identity)))], &actions);
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        let error = body.frame().await.unwrap().unwrap_err();
        assert!(
            matches!(error.downcast_ref::<RemoteSourceError<HttpError>>(), Some(RemoteSourceError::Source(error)) if error.code() == HttpErrorCode::BODY_PARSE_ERROR)
        );
        let Err(ApplicationBodyError::Application(error)) = completion.await else {
            panic!("original error retained")
        };
        assert!(Arc::ptr_eq(
            &error.downcast_ref::<OriginalError>().unwrap().0,
            &identity
        ));
        assert_dropped(&actions);

        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut app_body = TrackedBody::new(vec![], &actions);
        app_body.pending = true;
        app_body.panic_on_poll = true;
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        assert!(body.frame().await.unwrap().is_err());
        let Err(ApplicationBodyError::Task(error)) = completion.await else {
            panic!("task panic retained after join")
        };
        assert!(error.is_panic());
        assert_dropped(&actions);
    });
}

#[test]
fn frame_ceiling_rejects_large_frames_and_default_accepts_four_mib() {
    run_case(|worker, _handle| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"too large")))],
            &actions,
        );
        let (mut body, attachment, completion) = export(
            &worker,
            ApplicationBodyOptions {
                maximum_frame_bytes: NonZeroUsize::new(4).unwrap(),
                ..ApplicationBodyOptions::default()
            },
        )
        .await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        assert!(body.frame().await.unwrap().is_err());
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::FrameTooLarge {
                actual: 9,
                maximum: 4
            })
        ));
        assert_dropped(&actions);

        let actions = Arc::new(Mutex::new(Vec::new()));
        let bytes = Bytes::from(vec![7; 4 * 1024 * 1024]);
        let pointer = bytes.as_ptr();
        let app_body = TrackedBody::new(vec![Ok(Frame::data(bytes))], &actions);
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        let mut received = 0;
        while let Some(frame) = body.frame().await {
            let data = frame.unwrap().into_data().unwrap();
            assert!(data.len() <= 64 * 1024);
            assert_eq!(data.as_ptr(), pointer.wrapping_add(received));
            received += data.len();
        }
        assert_eq!(received, 4 * 1024 * 1024);
        completion.await.unwrap();
        assert_dropped(&actions);
    });
}

#[test]
fn pending_body_and_dropped_observer_are_joined_during_quiescence() {
    for grace in [Duration::ZERO, Duration::from_millis(20)] {
        run_case(|worker, handle| async move {
            let actions = Arc::new(Mutex::new(Vec::new()));
            let (polled, observed) = oneshot::channel();
            let mut app_body = TrackedBody::new(vec![], &actions);
            app_body.pending = true;
            *app_body.polled.borrow_mut() = Some(polled);
            let (mut body, attachment, completion) =
                export(&worker, ApplicationBodyOptions::default()).await;
            attachment
                .attach(app_body)
                .unwrap_or_else(|error| panic!("{:?}", error.reason));
            drop(completion);
            let mut read = Box::pin(body.frame());
            assert!(futures::poll!(&mut read).is_pending());
            observed.await.expect("producer parked in poll_frame");
            let quiescence = handle.quiesce(grace);
            drop(quiescence);
            handle
                .quiesce(grace)
                .await
                .expect("dropped observers do not cancel cleanup");
            assert_dropped(&actions);
            drop(read);
            drop(body);
        });
    }
}

#[test]
fn response_body_drop_joins_a_parked_application_producer() {
    run_case(|worker, _handle| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let mut app_body = TrackedBody::new(vec![], &actions);
        app_body.pending = true;
        *app_body.polled.borrow_mut() = Some(polled);
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        let mut read = Box::pin(body.frame());
        assert!(futures::poll!(&mut read).is_pending());
        observed.await.unwrap();
        drop(read);
        drop(body);
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::Cancelled)
        ));
        assert_dropped(&actions);
    });
}

#[test]
fn full_demand_queue_cancels_before_attachment_without_polling_a_body() {
    run_case(|worker, _handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let source = worker
            .call_local(move |context| async move {
                let (mut source, attachment, completion) = ApplicationBodySource::prepare(
                    &context,
                    message(),
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                let read = source.read_body_event(1);
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("app receives attachment");
                released.await.unwrap();
                drop(source);
                drop(read);
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        release.send(()).unwrap();
        source.await.unwrap();
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::Cancelled)
        ));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"never polled")))],
            &actions,
        );
        let rejected = attachment
            .attach(app_body)
            .expect_err("source closed before attachment");
        assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
        assert!(actions.lock().unwrap().is_empty());
        drop(rejected.body);
        assert_dropped(&actions);
    });
}

#[test]
fn graceful_admission_rejects_attachment_and_new_preparation() {
    run_case(|worker, handle| async move {
        let (body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        let (release, released) = oneshot::channel();
        let late = worker
            .call_local(move |context| async move {
                released.await.unwrap();
                ApplicationBodySource::prepare(
                    &context,
                    message(),
                    ApplicationBodyOptions::default(),
                )
                .err()
            })
            .unwrap();
        let quiescence = handle.quiesce(Duration::from_secs(60));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let rejected = attachment
            .attach(TrackedBody::new(vec![], &actions))
            .expect_err("grace closes attachment admission");
        assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
        drop(rejected.body);
        release.send(()).unwrap();
        assert!(late.await.unwrap().is_some());
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::Cancelled)
        ));
        drop(body);
        quiescence
            .await
            .expect("scopes finish without waiting for the long deadline");
        assert_dropped(&actions);
    });
}

#[test]
fn zero_size_hint_does_not_suppress_trailers_or_header_completion() {
    run_case(|worker, _handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let result = worker
            .call_local(move |context| async move {
                let (mut source, attachment, completion) = ApplicationBodySource::prepare(
                    &context,
                    message(),
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("app receives preparation");
                let header = source.read_header_event().await.unwrap();
                assert!(!header.eom(), "zero data length still permits trailers");
                assert_eq!(source.size_hint().unwrap().exact(), Some(0));
                let trailers = source.read_body_event(1).await.unwrap();
                assert!(trailers.eom());
                assert_eq!(source.size_hint().unwrap().exact(), Some(0));
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut trailers = HeaderMap::new();
        trailers.insert("x-result", HeaderValue::from_static("done"));
        let mut app_body = TrackedBody::new(vec![Ok(Frame::trailers(trailers))], &actions);
        app_body.hint = SizeHint::with_exact(0);
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        result.await.unwrap();
        completion.await.unwrap();
        assert_dropped(&actions);
    });
}

#[test]
fn empty_body_completes_without_polling_and_without_a_header_read() {
    run_case(|worker, _handle| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut app_body = TrackedBody::new(vec![], &actions);
        app_body.hint = SizeHint::with_exact(0);
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        completion
            .await
            .expect("empty body task joined without waiting for a read");
        assert!(
            body.frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .is_empty()
        );
        assert!(body.is_end_stream());
        assert!(
            !actions
                .lock()
                .unwrap()
                .iter()
                .any(|action| matches!(action, Action::Poll(_)))
        );
        assert_dropped(&actions);
    });
}

#[test]
fn graceful_quiescence_finishes_an_already_attached_body() {
    run_case(|worker, handle| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let (ready, readiness) = oneshot::channel();
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"final")))],
            &actions,
        );
        *app_body.polled.borrow_mut() = Some(polled);
        *app_body.ready.borrow_mut() = Some(readiness);
        let (mut body, attachment, completion) =
            export(&worker, ApplicationBodyOptions::default()).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        let mut read = Box::pin(body.frame());
        assert!(futures::poll!(&mut read).is_pending());
        observed.await.unwrap();
        let quiescence = handle.quiesce(Duration::from_secs(60));
        ready.send(()).unwrap();
        assert_eq!(
            read.await.unwrap().unwrap().into_data().unwrap(),
            Bytes::from_static(b"final")
        );
        completion
            .await
            .expect("accepted producer finishes normally during grace");
        quiescence
            .await
            .expect("both scope credits release before the deadline");
        assert_dropped(&actions);
    });
}

#[test]
fn preadmitted_response_binds_real_head_and_attaches_during_grace() {
    run_case(|worker, handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let (head, response_head) = oneshot::channel();
        let response = worker
            .call_local(move |context| async move {
                let (pending, attachment, completion) = ApplicationBodySource::prepare_response(
                    &context,
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("preparation delivered before service");
                let message = response_head.await.unwrap();
                assert!(!context.handle().is_accepting());
                assert!(
                    ApplicationBodySource::prepare_response(
                        &context,
                        ApplicationBodyOptions::default()
                    )
                    .is_err(),
                    "fresh response preparation stays closed during grace"
                );
                let mut source = pending.into_source(message);
                let mut header = source.read_header_event().await.unwrap();
                assert_eq!(header.headers_mut().status_code().unwrap(), 201);
                assert!(!header.eom());
                let mut chunks = Vec::new();
                for terminal in [false, true] {
                    let event = source.read_body_event(2).await.unwrap();
                    assert_eq!(event.eom(), terminal);
                    let HttpBodyEventKind::Body(body) = event.into_kind() else {
                        panic!("response delivers data")
                    };
                    chunks.extend(body.into_bytes_segments());
                }
                chunks
            })
            .expect("exchange admitted before service dispatch");
        let (attachment, completion) = preparation.await.unwrap();
        let quiescence = handle.quiesce(Duration::from_secs(60));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let payload = Bytes::from(vec![b'l', b'a', b't', b'e']);
        let pointer = payload.as_ptr();
        attachment
            .attach(TrackedBody::new(vec![Ok(Frame::data(payload))], &actions))
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        let mut message = message();
        message.set_status_code(201).unwrap();
        head.send(message).unwrap();
        let chunks = response
            .await
            .expect("previously admitted response completes");
        assert_eq!(
            chunks,
            vec![Bytes::from_static(b"la"), Bytes::from_static(b"te")]
        );
        assert_eq!(chunks[0].as_ptr(), pointer);
        assert_eq!(chunks[1].as_ptr(), pointer.wrapping_add(2));
        completion
            .await
            .expect("late-attached application task physically joined");
        quiescence
            .await
            .expect("scope credit released before the long deadline");
        assert_dropped(&actions);
    });
}

#[test]
fn preadmitted_response_rejects_attachment_after_forced_shutdown() {
    for grace in [Duration::ZERO, Duration::from_millis(20)] {
        run_case(|worker, handle| async move {
            let (prepared, preparation) = oneshot::channel();
            let (head, response_head) = oneshot::channel();
            let response = worker
                .call_local(move |context| async move {
                    let (pending, attachment, completion) =
                        ApplicationBodySource::prepare_response(
                            &context,
                            ApplicationBodyOptions::default(),
                        )
                        .unwrap();
                    prepared
                        .send((attachment, completion))
                        .ok()
                        .expect("preparation delivered");
                    let message = response_head.await.unwrap();
                    drop(pending.into_source(message));
                })
                .unwrap();
            let (attachment, completion) = preparation.await.unwrap();
            handle
                .quiesce(grace)
                .await
                .expect("unused body reservation cleanup joined");
            assert!(
                response.await.is_err(),
                "forced shutdown cancels outstanding service delivery"
            );
            assert!(matches!(
                completion.await,
                Err(ApplicationBodyError::Cancelled)
            ));
            assert!(!worker.is_accepting());
            let actions = Arc::new(Mutex::new(Vec::new()));
            let rejected = attachment
                .attach(TrackedBody::new(vec![], &actions))
                .expect_err("a pre-admitted permit cannot survive forced shutdown");
            assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
            assert!(actions.lock().unwrap().is_empty());
            drop(rejected.body);
            drop(head);
            assert_dropped(&actions);
        });
    }
}

#[test]
fn preadmitted_response_attached_during_grace_is_joined_on_forced_shutdown() {
    run_case(|worker, handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let (head, response_head) = oneshot::channel();
        let response = worker
            .call_local(move |context| async move {
                let (pending, attachment, completion) = ApplicationBodySource::prepare_response(
                    &context,
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("preparation delivered");
                let mut source = pending.into_source(response_head.await.unwrap());
                source.read_header_event().await.unwrap();
                let _result = source.read_body_event(1).await;
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        let quiescence = handle.quiesce(Duration::from_secs(60));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let mut body = TrackedBody::new(vec![], &actions);
        body.pending = true;
        *body.polled.borrow_mut() = Some(polled);
        attachment
            .attach(body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        head.send(message()).unwrap();
        observed
            .await
            .expect("late-attached body parked on the app runtime");
        drop(completion);
        worker.begin_shutdown();
        quiescence
            .await
            .expect("dropped completion cannot detach the app task");
        let _result = response.await;
        assert_dropped(&actions);
    });
}

#[test]
fn unused_response_attachment_releases_scope_and_cannot_be_reopened() {
    run_case(|worker, handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let (head, response_head) = oneshot::channel();
        let response = worker
            .call_local(move |context| async move {
                let (pending, attachment, completion) = ApplicationBodySource::prepare_response(
                    &context,
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("preparation delivered");
                let mut source = pending.into_source(response_head.await.unwrap());
                let Err(error) = source.read_header_event().await else {
                    panic!("binding a real head cannot reopen cancellation")
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        drop(attachment);
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::Cancelled)
        ));
        let quiescence = handle.quiesce(Duration::from_secs(60));
        head.send(message()).unwrap();
        response.await.unwrap();
        quiescence
            .await
            .expect("unused reservation does not wait for forced shutdown");
    });
}

#[test]
fn response_source_drop_races_attachment_without_losing_task_join() {
    run_case(|worker, _handle| async move {
        for _attempt in 0..16 {
            let (prepared, preparation) = oneshot::channel();
            let barrier = Arc::new(Barrier::new(2));
            let owner_barrier = Arc::clone(&barrier);
            let response = worker
                .call_local(move |context| async move {
                    let (pending, attachment, completion) =
                        ApplicationBodySource::prepare_response(
                            &context,
                            ApplicationBodyOptions::default(),
                        )
                        .unwrap();
                    prepared
                        .send((attachment, completion))
                        .ok()
                        .expect("preparation delivered");
                    owner_barrier.wait().await;
                    drop(pending);
                })
                .unwrap();
            let (attachment, completion) = preparation.await.unwrap();
            let actions = Arc::new(Mutex::new(Vec::new()));
            let mut body = TrackedBody::new(vec![], &actions);
            body.pending = true;
            barrier.wait().await;
            if let Err(rejected) = attachment.attach(body) {
                assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
                assert!(actions.lock().unwrap().is_empty());
                drop(rejected.body);
            }
            response.await.unwrap();
            assert!(matches!(
                completion.await,
                Err(ApplicationBodyError::Cancelled)
            ));
            assert_dropped(&actions);
        }
    });
}

#[test]
fn full_delivery_slot_and_retained_frame_do_not_block_cancellation() {
    run_case(|worker, _handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let source = worker
            .call_local(move |context| async move {
                let (mut source, attachment, completion) = ApplicationBodySource::prepare(
                    &context,
                    message(),
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                let read = source.read_body_event(1);
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("app receives attachment");
                released.await.unwrap();
                drop(source);
                drop(read);
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"retained")))],
            &actions,
        );
        *app_body.polled.borrow_mut() = Some(polled);
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        observed.await.unwrap();
        release.send(()).unwrap();
        source.await.unwrap();
        assert!(matches!(
            completion.await,
            Err(ApplicationBodyError::Cancelled)
        ));
        assert_dropped(&actions);
        assert_eq!(
            actions
                .lock()
                .unwrap()
                .iter()
                .filter(|action| matches!(action, Action::Poll(_)))
                .count(),
            1
        );
    });
}

#[test]
fn source_size_hint_decreases_by_delivered_chunks() {
    run_case(|worker, _handle| async move {
        let (prepared, preparation) = oneshot::channel();
        let source = worker
            .call_local(move |context| async move {
                let (mut source, attachment, completion) = ApplicationBodySource::prepare(
                    &context,
                    message(),
                    ApplicationBodyOptions::default(),
                )
                .unwrap();
                assert!(source.size_hint().is_none());
                prepared
                    .send((attachment, completion))
                    .ok()
                    .expect("app receives attachment");
                assert!(!source.read_header_event().await.unwrap().eom());
                assert_eq!(source.size_hint().unwrap().exact(), Some(8));
                for remaining in [5, 2, 0] {
                    let event = source.read_body_event(3).await.unwrap();
                    assert_eq!(source.size_hint().unwrap().exact(), Some(remaining));
                    assert_eq!(event.eom(), remaining == 0);
                }
            })
            .unwrap();
        let (attachment, completion) = preparation.await.unwrap();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"12345678")))],
            &actions,
        );
        app_body.hint = SizeHint::with_exact(8);
        attachment
            .attach(app_body)
            .unwrap_or_else(|error| panic!("{:?}", error.reason));
        source.await.unwrap();
        completion.await.unwrap();
        assert_dropped(&actions);
    });
}

#[test]
fn trailer_rejection_is_bounded_and_never_silently_trims() {
    run_case(|worker, _handle| async move {
        for (value, ceiling) in [("long value", 4), (" whitespace ", 64)] {
            let actions = Arc::new(Mutex::new(Vec::new()));
            let mut trailers = HeaderMap::new();
            trailers.insert("x-end", HeaderValue::from_static(value));
            let app_body = TrackedBody::new(vec![Ok(Frame::trailers(trailers))], &actions);
            let (mut body, attachment, completion) = export(
                &worker,
                ApplicationBodyOptions {
                    maximum_frame_bytes: NonZeroUsize::new(ceiling).unwrap(),
                    ..ApplicationBodyOptions::default()
                },
            )
            .await;
            attachment
                .attach(app_body)
                .unwrap_or_else(|error| panic!("{:?}", error.reason));
            assert!(body.frame().await.unwrap().is_err());
            match completion.await {
                Err(ApplicationBodyError::FrameTooLarge { maximum: 4, .. }) if ceiling == 4 => {}
                Err(ApplicationBodyError::InvalidTrailers) if ceiling == 64 => {}
                outcome => panic!("unexpected trailer outcome: {outcome:?}"),
            }
            assert_dropped(&actions);
        }
    });
}

#[test]
fn attachment_without_an_application_runtime_returns_the_untouched_body() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .unwrap();
    let handle = owner.handle();
    let worker = handle.worker(0).unwrap();
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let (body, attachment, completion, app_body) = app.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let (body, attachment, completion) =
                export(&worker, ApplicationBodyOptions::default()).await;
            (
                body,
                attachment,
                completion,
                TrackedBody::new(vec![], &actions),
            )
        })
        .await
        .expect("preparation bounded")
    });
    let rejected = attachment
        .attach(app_body)
        .expect_err("runtime entry required");
    assert_eq!(
        rejected.reason,
        ApplicationBodyAttachError::NoApplicationRuntime
    );
    assert!(actions.lock().unwrap().is_empty());
    app.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, async {
            drop(rejected.body);
            drop(body);
            assert!(matches!(
                completion.await,
                Err(ApplicationBodyError::Cancelled)
            ));
            handle.quiesce(Duration::ZERO).await.unwrap();
        })
        .await
        .expect("cleanup bounded");
    });
    owner.join().unwrap();
    assert_dropped(&actions);
}

type AppJoinParent = JoinSet<Result<(), ApplicationBodyError>>;

fn run_join_case<Work: Future<Output = ()>>(
    case: impl FnOnce(WorkerHandle, RuntimeHandle) -> Work,
) {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .unwrap();
    let runtime = owner.handle();
    let worker = runtime.worker(0).unwrap();
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    app.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, async {
            case(worker, runtime.clone()).await;
            runtime
                .quiesce(TEST_TIMEOUT)
                .await
                .expect("all scope credits retire during grace");
        })
        .await
        .expect("application joins remain bounded");
    });
    let report = owner.join().expect("physical worker join outside Tokio");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

async fn export_join(
    worker: &WorkerHandle,
    options: ApplicationBodyOptions,
) -> (RoxygenBody, ApplicationBodyJoinPreparation) {
    worker
        .call(move |context| {
            let (source, preparation) =
                ApplicationBodySource::prepare_with_application_join(context, message(), options)
                    .unwrap();
            let body =
                RoxygenBody::try_from_local(context, Box::new(source), LocalBodyOptions::default())
                    .unwrap();
            (body, preparation)
        })
        .unwrap()
        .await
        .unwrap()
}

async fn arm_join(
    preparation: ApplicationBodyJoinPreparation,
) -> (
    ApplicationBodyAttachment,
    AppJoinParent,
    ApplicationBodyCleanupObserver,
) {
    let (driver, ready, cleanup) = preparation.into_parts();
    let mut parent = JoinSet::new();
    parent.spawn(driver);
    let attachment = ready
        .await
        .expect("parent arms attachment before admission");
    (attachment, parent, cleanup)
}

async fn reap_driver(parent: &mut AppJoinParent) -> Result<(), ApplicationBodyError> {
    let result = parent
        .join_next()
        .await
        .expect("parent retains its driver")
        .expect("driver task returns");
    assert!(
        parent.join_next().await.is_none(),
        "exactly one caller-owned driver"
    );
    result
}

enum NativeJoinCommand {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
    Drop(oneshot::Sender<()>),
}

struct ManualJoinWorker {
    worker: WorkerHandle,
    commands: mpsc::Sender<NativeJoinCommand>,
}

impl ManualJoinWorker {
    async fn pause(&self) {
        let (sender, received) = oneshot::channel();
        self.commands
            .send(NativeJoinCommand::Pause(sender))
            .await
            .unwrap();
        received.await.expect("native polling is parked");
    }

    async fn resume(&self) {
        let (sender, received) = oneshot::channel();
        self.commands
            .send(NativeJoinCommand::Resume(sender))
            .await
            .unwrap();
        received.await.expect("native polling resumes");
    }

    async fn stop(&self) {
        let (sender, received) = oneshot::channel();
        self.commands
            .send(NativeJoinCommand::Drop(sender))
            .await
            .unwrap();
        received
            .await
            .expect("actual native driver destruction finished");
        assert_eq!(self.worker.state(), WorkerState::Closed);
    }
}

fn run_manual_join_case<Work: Future<Output = ()>>(case: impl FnOnce(ManualJoinWorker) -> Work) {
    let (started, startup) = oneshot::channel();
    let (commands, mut incoming) = mpsc::channel(1);
    let native = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(LocalSet::new().run_until(async move {
            let (worker, driver) = WorkerDriver::new(4).unwrap();
            started.send(worker).expect("application receives worker");
            let mut driver = Box::pin(driver);
            let mut paused = false;
            loop {
                tokio::select! {
                    biased;
                    command = incoming.recv() => match command {
                        Some(NativeJoinCommand::Pause(reply)) => {
                            paused = true;
                            let _ = reply.send(());
                        }
                        Some(NativeJoinCommand::Resume(reply)) => {
                            paused = false;
                            let _ = reply.send(());
                        }
                        Some(NativeJoinCommand::Drop(reply)) => {
                            drop(driver);
                            let _ = reply.send(());
                            return;
                        }
                        None => return,
                    },
                    () = &mut driver, if !paused => return,
                }
            }
        }));
    });
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        app.block_on(async move {
            tokio::time::timeout(TEST_TIMEOUT, async move {
                let worker = startup.await.unwrap();
                case(ManualJoinWorker { worker, commands }).await;
            })
            .await
            .expect("native driver destruction and app joins are bounded");
        })
    }));
    native
        .join()
        .expect("native thread physically joined while app runtime lives");
    if let Err(payload) = outcome {
        resume_unwind(payload);
    }
}

#[test]
fn application_owned_join_requires_parent_before_attachment() {
    fn assert_send<Value: Send>() {}
    fn assert_observer<Value: Send + Sync + Clone>() {}
    assert_send::<ApplicationBodyJoinPreparation>();
    assert_send::<ApplicationBodyJoinDriver>();
    assert_send::<ApplicationBodyAttachmentReady>();
    assert_observer::<ApplicationBodyCleanupObserver>();
    run_join_case(|worker, _runtime| async move {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(b"armed")))],
            &actions,
        );
        let (mut body, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
        let (driver, ready, cleanup) = preparation.into_parts();
        let mut ready = Box::pin(ready);
        assert!(futures::poll!(&mut ready).is_pending());
        assert!(actions.lock().unwrap().is_empty());
        assert!(!cleanup.joining());
        let mut parent = JoinSet::new();
        parent.spawn(driver);
        let attachment = ready.as_mut().await.unwrap();
        drop(ready);
        attachment
            .attach(app_body)
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            Bytes::from_static(b"armed")
        );
        reap_driver(&mut parent).await.unwrap();
        assert_eq!(cleanup.reaped(), 1);
        fn send_future(_: impl Future + Send + 'static) {}
        send_future(cleanup.wait());
        cleanup.wait().await.unwrap();
        assert_dropped(&actions);
    });
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .unwrap();
    let runtime = owner.handle();
    let worker = runtime.worker(0).unwrap();
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (body, preparation) = app.block_on(async {
        tokio::time::timeout(
            TEST_TIMEOUT,
            export_join(&worker, ApplicationBodyOptions::default()),
        )
        .await
        .unwrap()
    });
    let (driver, ready, cleanup) = preparation.into_parts();
    let mut driver = Box::pin(driver);
    assert!(matches!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ApplicationBodyError::Cancelled))
    ));
    drop(driver);
    app.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, async {
            assert!(matches!(
                ready.await,
                Err(ApplicationBodyAttachError::NoApplicationRuntime)
            ));
            cleanup
                .wait()
                .await
                .expect("unarmed rejection certifies the empty group");
            assert_eq!(cleanup.reaped(), 0);
            drop(body);
            runtime.quiesce(TEST_TIMEOUT).await.unwrap();
        })
        .await
        .unwrap();
    });
    let report = owner.join().unwrap();
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

#[test]
fn application_owned_join_unused_endpoints_retire_without_spawn() {
    run_join_case(|worker, _runtime| async move {
        for mode in 0..4 {
            let (body, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
            let (driver, ready, cleanup) = preparation.into_parts();
            if mode == 0 {
                drop(driver);
                assert!(matches!(
                    ready.await,
                    Err(ApplicationBodyAttachError::Closed)
                ));
            } else {
                let mut parent = JoinSet::new();
                if mode == 3 {
                    let mut driver = Box::pin(driver);
                    assert!(futures::poll!(&mut driver).is_pending());
                    let attachment = ready.await.unwrap();
                    drop(driver);
                    let actions = Arc::new(Mutex::new(Vec::new()));
                    let rejected = attachment
                        .attach(TrackedBody::new(vec![], &actions))
                        .expect_err("lost owner closes admission");
                    assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
                    assert!(actions.lock().unwrap().is_empty());
                    drop(rejected.body);
                    assert_dropped(&actions);
                } else {
                    parent.spawn(driver);
                    if mode == 1 {
                        drop(ready);
                    } else {
                        drop(ready.await.unwrap());
                    }
                    assert!(matches!(
                        reap_driver(&mut parent).await,
                        Err(ApplicationBodyError::Cancelled)
                    ));
                }
            }
            cleanup
                .wait()
                .await
                .expect("closed empty group and real scope retire");
            cleanup
                .wait()
                .await
                .expect("empty-group certification is sticky");
            assert_eq!(cleanup.reaped(), 0);
            drop(body);
        }
    });
}

#[test]
fn application_owned_join_rejects_late_attachment() {
    run_join_case(|worker, _runtime| async move {
        for accepted in [false, true] {
            let (mut body, preparation) =
                export_join(&worker, ApplicationBodyOptions::default()).await;
            let (attachment, mut parent, cleanup) = arm_join(preparation).await;
            let actions = Arc::new(Mutex::new(Vec::new()));
            let (polled, observed) = oneshot::channel();
            let mut app_body = TrackedBody::new(vec![], &actions);
            app_body.pending = true;
            *app_body.polled.borrow_mut() = Some(polled);
            if accepted {
                attachment
                    .attach(app_body)
                    .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
                let mut read = Box::pin(body.frame());
                assert!(futures::poll!(&mut read).is_pending());
                observed.await.unwrap();
                drop(read);
                drop(body);
            } else {
                drop(body);
                cleanup
                    .wait()
                    .await
                    .expect("source cancellation closes the empty group");
                let rejected = attachment
                    .attach(app_body)
                    .expect_err("cancelled source rejects untouched body");
                assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
                assert!(Arc::ptr_eq(&rejected.body.actions, &actions));
                assert!(actions.lock().unwrap().is_empty());
                drop(rejected.body);
            }
            assert!(matches!(
                reap_driver(&mut parent).await,
                Err(ApplicationBodyError::Cancelled)
            ));
            cleanup.wait().await.unwrap();
            assert_eq!(cleanup.reaped(), usize::from(accepted));
            assert_dropped(&actions);
        }
    });
}

#[test]
fn application_owned_response_join_attaches_during_grace() {
    run_join_case(|worker, runtime| async move {
        let (prepared, preparation) = oneshot::channel();
        let (head, response_head) = oneshot::channel();
        let response = worker
            .call_local(move |context| async move {
                let (pending, preparation) =
                    ApplicationBodySource::prepare_response_with_application_join(
                        &context,
                        ApplicationBodyOptions::default(),
                    )
                    .unwrap();
                prepared
                    .send(preparation)
                    .ok()
                    .expect("response reserved before service");
                let actual_head = response_head.await.unwrap();
                assert!(!context.handle().is_accepting());
                assert!(
                    ApplicationBodySource::prepare_response_with_application_join(
                        &context,
                        ApplicationBodyOptions::default()
                    )
                    .is_err()
                );
                let mut source = pending.into_source(actual_head);
                let mut header = source.read_header_event().await.unwrap();
                assert_eq!(header.headers_mut().status_code().unwrap(), 201);
                assert!(!header.eom());
                let mut chunks = Vec::new();
                for terminal in [false, true] {
                    let event = source.read_body_event(2).await.unwrap();
                    assert_eq!(event.eom(), terminal);
                    let HttpBodyEventKind::Body(bytes) = event.into_kind() else {
                        panic!("data")
                    };
                    chunks.extend(bytes.into_bytes_segments());
                }
                chunks
            })
            .unwrap();
        let reserved = preparation.await.unwrap();
        let (ordinary, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
        let (ordinary_attachment, mut ordinary_parent, ordinary_cleanup) =
            arm_join(preparation).await;
        let (driver, ready, cleanup) = reserved.into_parts();
        let mut parent = JoinSet::new();
        parent.spawn(driver);
        let quiescence = runtime.quiesce(TEST_TIMEOUT);
        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        let actions = Arc::new(Mutex::new(Vec::new()));
        let rejected = ordinary_attachment
            .attach(TrackedBody::new(vec![], &actions))
            .expect_err("ordinary admission closes during grace");
        assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
        assert!(actions.lock().unwrap().is_empty());
        drop(rejected.body);
        assert!(matches!(
            reap_driver(&mut ordinary_parent).await,
            Err(ApplicationBodyError::Cancelled)
        ));
        ordinary_cleanup.wait().await.unwrap();
        drop(ordinary);
        let actions = Arc::new(Mutex::new(Vec::new()));
        let payload = Bytes::from(vec![b'l', b'a', b't', b'e']);
        let pointer = payload.as_ptr();
        ready
            .await
            .unwrap()
            .attach(TrackedBody::new(vec![Ok(Frame::data(payload))], &actions))
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        let mut actual_head = message();
        actual_head.set_status_code(201).unwrap();
        head.send(actual_head).unwrap();
        let chunks = response.await.unwrap();
        assert_eq!(
            chunks,
            vec![Bytes::from_static(b"la"), Bytes::from_static(b"te")]
        );
        assert_eq!(chunks[0].as_ptr(), pointer);
        assert_eq!(chunks[1].as_ptr(), pointer.wrapping_add(2));
        reap_driver(&mut parent).await.unwrap();
        cleanup.wait().await.unwrap();
        quiescence.await.unwrap();
        assert_dropped(&actions);
    });
}

async fn parked_join_after_native_loss(native: ManualJoinWorker, gate_reaping: bool) {
    let (mut body, preparation) =
        export_join(&native.worker, ApplicationBodyOptions::default()).await;
    let (driver, ready, cleanup) = preparation.into_parts();
    let mut driver = Box::pin(driver);
    assert!(futures::poll!(&mut driver).is_pending());
    let actions = Arc::new(Mutex::new(Vec::new()));
    let (polled, observed) = oneshot::channel();
    let (dropped, destruction) = oneshot::channel();
    let mut app_body = TrackedBody::new(vec![], &actions);
    app_body.pending = true;
    app_body.dropped = Some(dropped);
    *app_body.polled.borrow_mut() = Some(polled);
    ready
        .await
        .unwrap()
        .attach(app_body)
        .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
    let mut read = Box::pin(body.frame());
    assert!(futures::poll!(&mut read).is_pending());
    observed
        .await
        .expect("actual producer is pending in poll_frame");
    assert!(futures::poll!(&mut driver).is_pending());
    assert!(
        cleanup.joining(),
        "app driver has acquired the actual producer group"
    );
    assert_eq!(cleanup.reaped(), 0);
    let (release, released) = oneshot::channel();
    let mut parent = JoinSet::new();
    parent.spawn(async move {
        released.await.expect("parent retains parked join driver");
        driver.await
    });
    native.stop().await;
    destruction
        .await
        .expect("native loss cancels the app producer on its live runtime");
    assert_dropped(&actions);
    assert_eq!(cleanup.reaped(), 0, "body destruction is not group reaping");
    if gate_reaping {
        let mut observation = Box::pin(cleanup.wait());
        assert!(
            futures::poll!(&mut observation).is_pending(),
            "actual native scope is gone but producer join remains owed"
        );
        drop(observation);
    }
    release.send(()).unwrap();
    assert!(matches!(
        reap_driver(&mut parent).await,
        Err(ApplicationBodyError::Cancelled)
    ));
    assert_eq!(cleanup.reaped(), 1);
    cleanup
        .wait()
        .await
        .expect("independent app reaping survives physical native-driver destruction");
    drop(read);
    drop(body);
}

#[test]
fn application_owned_join_survives_worker_driver_drop() {
    run_manual_join_case(|native| parked_join_after_native_loss(native, false));
    for armed in [false, true] {
        run_manual_join_case(move |native| async move {
            let (body, preparation) =
                export_join(&native.worker, ApplicationBodyOptions::default()).await;
            let (driver, ready, cleanup) = preparation.into_parts();
            let mut parent = JoinSet::new();
            if armed {
                parent.spawn(driver);
                let attachment = ready.await.unwrap();
                native.stop().await;
                let actions = Arc::new(Mutex::new(Vec::new()));
                let rejected = attachment
                    .attach(TrackedBody::new(vec![], &actions))
                    .expect_err("forced native loss closes an armed attachment");
                assert_eq!(rejected.reason, ApplicationBodyAttachError::Closed);
                assert!(actions.lock().unwrap().is_empty());
                drop(rejected.body);
                assert_dropped(&actions);
            } else {
                native.stop().await;
                parent.spawn(driver);
                assert!(matches!(
                    ready.await,
                    Err(ApplicationBodyAttachError::Closed)
                ));
            }
            assert!(matches!(
                reap_driver(&mut parent).await,
                Err(ApplicationBodyError::Cancelled)
            ));
            cleanup.wait().await.unwrap();
            assert_eq!(cleanup.reaped(), 0);
            drop(body);
        });
    }
}

#[test]
fn application_owned_join_waits_for_reap_after_body_drop() {
    run_manual_join_case(|native| parked_join_after_native_loss(native, true));
}

#[test]
fn application_owned_join_waits_for_worker_scope_retirement() {
    run_manual_join_case(|native| async move {
        let (body, preparation) =
            export_join(&native.worker, ApplicationBodyOptions::default()).await;
        native.pause().await;
        let (attachment, mut parent, cleanup) = arm_join(preparation).await;
        let actions = Arc::new(Mutex::new(Vec::new()));
        attachment
            .attach(TrackedBody::new(vec![], &actions))
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        reap_driver(&mut parent).await.unwrap();
        assert_dropped(&actions);
        assert_eq!(cleanup.reaped(), 1);
        let mut observation = Box::pin(cleanup.wait());
        assert!(
            futures::poll!(&mut observation).is_pending(),
            "producer and enclosing driver joined, real scope not yet retired"
        );
        native.resume().await;
        observation.await.unwrap();
        cleanup.wait().await.unwrap();
        drop(body);
        native.stop().await;
    });
}

#[test]
fn application_owned_join_preserves_original_error_and_task_panic() {
    for panic in [false, true] {
        run_manual_join_case(move |native| async move {
            let (mut body, preparation) =
                export_join(&native.worker, ApplicationBodyOptions::default()).await;
            let (driver, ready, cleanup) = preparation.into_parts();
            let mut driver = Box::pin(driver);
            assert!(futures::poll!(&mut driver).is_pending());
            let actions = Arc::new(Mutex::new(Vec::new()));
            let identity = Arc::new(());
            let (dropped, destruction) = oneshot::channel();
            let mut app_body =
                TrackedBody::new(vec![Err(OriginalError(Arc::clone(&identity)))], &actions);
            app_body.panic_on_poll = panic;
            app_body.dropped = Some(dropped);
            ready
                .await
                .unwrap()
                .attach(app_body)
                .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
            assert!(futures::poll!(&mut driver).is_pending());
            assert!(cleanup.joining());
            let error = body.frame().await.unwrap().unwrap_err();
            assert!(
                matches!(error.downcast_ref::<RemoteSourceError<HttpError>>(), Some(RemoteSourceError::Source(error)) if error.code() == HttpErrorCode::BODY_PARSE_ERROR)
            );
            destruction.await.unwrap();
            assert_eq!(cleanup.reaped(), 0);
            let (release, released) = oneshot::channel();
            let mut parent = JoinSet::new();
            parent.spawn(async move {
                released.await.unwrap();
                driver.await
            });
            native.stop().await;
            let mut observation = Box::pin(cleanup.wait());
            assert!(futures::poll!(&mut observation).is_pending());
            release.send(()).unwrap();
            match reap_driver(&mut parent).await {
                Err(ApplicationBodyError::Application(error)) if !panic => {
                    assert!(Arc::ptr_eq(
                        &error.downcast_ref::<OriginalError>().unwrap().0,
                        &identity
                    ));
                }
                Err(ApplicationBodyError::Task(error)) if panic => {
                    assert!(error.is_panic());
                    let payload = error.into_panic();
                    assert_eq!(
                        payload.downcast_ref::<&str>(),
                        Some(&"deliberate producer panic")
                    );
                }
                outcome => panic!("original joined outcome was replaced: {outcome:?}"),
            }
            assert_eq!(cleanup.reaped(), 1);
            observation
                .await
                .expect("joined failure is not uncertain cleanup");
            assert_dropped(&actions);
            drop(body);
        });
    }
}

#[test]
fn application_owned_join_observer_drop_does_not_cancel() {
    run_join_case(|worker, _runtime| async move {
        let (mut body, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
        let (attachment, mut parent, cleanup) = arm_join(preparation).await;
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let app_body = TrackedBody::new(
            vec![Ok(Frame::data(Bytes::from_static(
                b"observed independently",
            )))],
            &actions,
        );
        *app_body.polled.borrow_mut() = Some(polled);
        *app_body.ready.borrow_mut() = Some(released);
        attachment
            .attach(app_body)
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        let mut read = Box::pin(body.frame());
        assert!(futures::poll!(&mut read).is_pending());
        observed.await.unwrap();
        let mut observation = Box::pin(cleanup.wait());
        assert!(futures::poll!(&mut observation).is_pending());
        let clone = cleanup.clone();
        drop(observation);
        drop(cleanup);
        drop(clone);
        assert!(!release.is_closed());
        release.send(()).unwrap();
        assert_eq!(
            read.await.unwrap().unwrap().into_data().unwrap(),
            Bytes::from_static(b"observed independently")
        );
        reap_driver(&mut parent).await.unwrap();
        assert_dropped(&actions);
    });
}

#[test]
fn application_owned_join_preserves_streaming_bounds_and_affinity() {
    run_join_case(|worker, _runtime| async move {
        let context = RuntimeContext::new();
        let _context = context.enter();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let bytes = Bytes::from(vec![b'x'; 16]).slice(3..11);
        let pointer = bytes.as_ptr();
        let mut trailers = HeaderMap::new();
        trailers.append("x-result", HeaderValue::from_static("first"));
        trailers.append("x-result", HeaderValue::from_static("second"));
        let mut app_body = TrackedBody::new(
            vec![Ok(Frame::data(bytes)), Ok(Frame::trailers(trailers))],
            &actions,
        );
        app_body.hint = SizeHint::with_exact(8);
        let (mut body, preparation) = export_join(
            &worker,
            ApplicationBodyOptions {
                maximum_demand_bytes: NonZeroU32::new(3).unwrap(),
                ..ApplicationBodyOptions::default()
            },
        )
        .await;
        let (attachment, mut parent, cleanup) = arm_join(preparation).await;
        attachment
            .attach(app_body)
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        let mut offset = 0;
        for expected in [3, 3, 2] {
            let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(data.len(), expected);
            assert_eq!(data.as_ptr(), pointer.wrapping_add(offset));
            offset += expected;
        }
        let trailers = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_trailers()
            .unwrap();
        assert_eq!(
            trailers
                .get_all("x-result")
                .iter()
                .map(|value| value.as_bytes())
                .collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
        reap_driver(&mut parent).await.unwrap();
        cleanup.wait().await.unwrap();
        assert_dropped(&actions);
        assert_eq!(
            actions
                .lock()
                .unwrap()
                .iter()
                .filter(|action| matches!(action, Action::Poll(_)))
                .count(),
            2
        );

        let (prepared, preparation) = oneshot::channel();
        let consumer = worker
            .call_local(move |context| async move {
                let (mut source, preparation) =
                    ApplicationBodySource::prepare_with_application_join(
                        &context,
                        message(),
                        ApplicationBodyOptions::default(),
                    )
                    .unwrap();
                prepared.send(preparation).ok().unwrap();
                assert!(!source.read_header_event().await.unwrap().eom());
                assert_eq!(source.size_hint().unwrap().exact(), Some(0));
                let event = source.read_body_event(1).await.unwrap();
                assert!(event.eom());
                assert!(matches!(event.into_kind(), HttpBodyEventKind::Trailers(_)));
                assert_eq!(source.size_hint().unwrap().exact(), Some(0));
            })
            .unwrap();
        let (attachment, mut parent, cleanup) = arm_join(preparation.await.unwrap()).await;
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut trailers = HeaderMap::new();
        trailers.insert("x-end", HeaderValue::from_static("zero-hint"));
        let mut app_body = TrackedBody::new(vec![Ok(Frame::trailers(trailers))], &actions);
        app_body.hint = SizeHint::with_exact(0);
        attachment
            .attach(app_body)
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        consumer.await.unwrap();
        reap_driver(&mut parent).await.unwrap();
        cleanup.wait().await.unwrap();
        assert_dropped(&actions);

        for oversized in [false, true] {
            let actions = Arc::new(Mutex::new(Vec::new()));
            let bytes = if oversized {
                Bytes::from_static(b"too large")
            } else {
                Bytes::from(vec![7; 4 * 1024 * 1024])
            };
            let pointer = bytes.as_ptr();
            let options = if oversized {
                ApplicationBodyOptions {
                    maximum_frame_bytes: NonZeroUsize::new(4).unwrap(),
                    ..ApplicationBodyOptions::default()
                }
            } else {
                ApplicationBodyOptions::default()
            };
            let (mut body, preparation) = export_join(&worker, options).await;
            let (attachment, mut parent, cleanup) = arm_join(preparation).await;
            attachment
                .attach(TrackedBody::new(vec![Ok(Frame::data(bytes))], &actions))
                .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
            if oversized {
                assert!(body.frame().await.unwrap().is_err());
                assert!(matches!(
                    reap_driver(&mut parent).await,
                    Err(ApplicationBodyError::FrameTooLarge {
                        actual: 9,
                        maximum: 4
                    })
                ));
            } else {
                let mut received = 0;
                while let Some(frame) = body.frame().await {
                    let bytes = frame.unwrap().into_data().unwrap();
                    assert!(bytes.len() <= 64 * 1024);
                    assert_eq!(bytes.as_ptr(), pointer.wrapping_add(received));
                    received += bytes.len();
                }
                assert_eq!(received, 4 * 1024 * 1024);
                reap_driver(&mut parent).await.unwrap();
            }
            cleanup.wait().await.unwrap();
            assert_dropped(&actions);
        }
        for (value, maximum) in [("too long", 4), (" whitespace ", 64)] {
            let actions = Arc::new(Mutex::new(Vec::new()));
            let mut trailers = HeaderMap::new();
            trailers.insert("x-end", HeaderValue::from_static(value));
            let (mut body, preparation) = export_join(
                &worker,
                ApplicationBodyOptions {
                    maximum_frame_bytes: NonZeroUsize::new(maximum).unwrap(),
                    ..ApplicationBodyOptions::default()
                },
            )
            .await;
            let (attachment, mut parent, cleanup) = arm_join(preparation).await;
            attachment
                .attach(TrackedBody::new(
                    vec![Ok(Frame::trailers(trailers))],
                    &actions,
                ))
                .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
            assert!(body.frame().await.unwrap().is_err());
            match reap_driver(&mut parent).await {
                Err(ApplicationBodyError::FrameTooLarge { maximum: 4, .. }) if maximum == 4 => {}
                Err(ApplicationBodyError::InvalidTrailers) if maximum == 64 => {}
                outcome => panic!("original trailer failure retained: {outcome:?}"),
            }
            cleanup.wait().await.unwrap();
            assert_dropped(&actions);
        }
    });
}

struct JoinScopeDropPanic;

impl Drop for JoinScopeDropPanic {
    fn drop(&mut self) {
        panic!("deliberate scope-capture destruction failure");
    }
}

async fn failed_join_scope(worker: &WorkerHandle) -> WorkerCleanupObserver {
    worker
        .call(|context| {
            let capture = JoinScopeDropPanic;
            let scope = context
                .register_scope(async move {
                    let _capture = capture;
                    std::future::pending::<()>().await;
                })
                .unwrap();
            let observer = scope.observe();
            assert!(catch_unwind(AssertUnwindSafe(|| scope.disarm())).is_err());
            observer
        })
        .unwrap()
        .await
        .unwrap()
}

async fn held_join_scope(worker: &WorkerHandle) -> (WorkerCleanupObserver, oneshot::Sender<()>) {
    let (release, released) = oneshot::channel();
    let observer = worker
        .call(move |context| {
            let scope = context
                .register_scope(async move {
                    released.await.unwrap();
                })
                .unwrap();
            let observer = scope.observe();
            drop(scope.activate().unwrap());
            observer
        })
        .unwrap()
        .await
        .unwrap();
    (observer, release)
}

struct JoinNotificationPanic;

impl Drop for JoinNotificationPanic {
    fn drop(&mut self) {
        panic!("secondary notification payload destructor failure");
    }
}

struct PanickingJoinWake;

impl Wake for PanickingJoinWake {
    fn wake(self: Arc<Self>) {
        panic_any(JoinNotificationPanic);
    }
}

#[derive(Default)]
struct CountJoinWake(AtomicUsize);

impl Wake for CountJoinWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn application_owned_join_loss_is_sticky_unconfirmed() {
    run_join_case(|worker, _runtime| async move {
        let (scope, release) = held_join_scope(&worker).await;
        let (cleanup, publisher) = ApplicationBodyCleanupObserver::test_observation(scope);
        drop(publisher);
        cleanup.test_publish(true);
        let mut observation = Box::pin(cleanup.wait());
        assert!(
            futures::poll!(&mut observation).is_pending(),
            "producer loss cannot short-circuit scope retirement"
        );
        release.send(()).unwrap();
        let error = observation.await.unwrap_err();
        assert!(error.producer_join_unconfirmed());
        assert_eq!(error.worker_scope_error(), None);
        assert_eq!(cleanup.wait().await, Err(error));
        for producer_lost in [false, true] {
            let scope = failed_join_scope(&worker).await;
            let (cleanup, publisher) = ApplicationBodyCleanupObserver::test_observation(scope);
            let mut observation = Box::pin(cleanup.wait());
            assert!(
                futures::poll!(&mut observation).is_pending(),
                "scope failure cannot short-circuit the producer certificate"
            );
            if producer_lost {
                drop(publisher);
            } else {
                publisher.confirm();
            }
            let error = observation.await.unwrap_err();
            assert_eq!(error.producer_join_unconfirmed(), producer_lost);
            assert_eq!(
                error.worker_scope_error(),
                Some(WorkerCleanupObservationError::Unconfirmed)
            );
            assert_eq!(cleanup.wait().await, Err(error));
        }
        let (scope, release) = held_join_scope(&worker).await;
        let (cleanup, publisher) = ApplicationBodyCleanupObserver::test_observation(scope);
        let mut panicking = Box::pin(cleanup.wait());
        let mut independent = Box::pin(cleanup.wait());
        let count = Arc::new(CountJoinWake::default());
        assert!(
            panicking
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::new(
                    PanickingJoinWake
                ))))
                .is_pending()
        );
        assert!(
            independent
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(&count))))
                .is_pending()
        );
        publisher.confirm();
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            1,
            "one panicking notification cannot skip another"
        );
        drop(panicking);
        drop(independent);
        release.send(()).unwrap();
        cleanup.wait().await.unwrap();
    });
}

#[test]
fn application_owned_join_independent_bodies_retire_separately() {
    run_join_case(|worker, _runtime| async move {
        let (mut slow, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
        let (attachment, mut slow_parent, slow_cleanup) = arm_join(preparation).await;
        let slow_actions = Arc::new(Mutex::new(Vec::new()));
        let (polled, observed) = oneshot::channel();
        let mut app_body = TrackedBody::new(vec![], &slow_actions);
        app_body.pending = true;
        *app_body.polled.borrow_mut() = Some(polled);
        attachment
            .attach(app_body)
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        let mut slow_read = Box::pin(slow.frame());
        assert!(futures::poll!(&mut slow_read).is_pending());
        observed.await.unwrap();
        let (mut fast, preparation) = export_join(&worker, ApplicationBodyOptions::default()).await;
        let (attachment, mut fast_parent, fast_cleanup) = arm_join(preparation).await;
        let fast_actions = Arc::new(Mutex::new(Vec::new()));
        let identity = Arc::new(());
        attachment
            .attach(TrackedBody::new(
                vec![Err(OriginalError(Arc::clone(&identity)))],
                &fast_actions,
            ))
            .unwrap_or_else(|rejected| panic!("{:?}", rejected.reason));
        assert!(fast.frame().await.unwrap().is_err());
        let Err(ApplicationBodyError::Application(error)) = reap_driver(&mut fast_parent).await
        else {
            panic!("independent original error retained")
        };
        assert!(Arc::ptr_eq(
            &error.downcast_ref::<OriginalError>().unwrap().0,
            &identity
        ));
        fast_cleanup.wait().await.unwrap();
        assert_eq!(fast_cleanup.reaped(), 1);
        assert_eq!(slow_cleanup.reaped(), 0);
        let mut pending = Box::pin(slow_cleanup.wait());
        assert!(futures::poll!(&mut pending).is_pending());
        assert!(
            !slow_actions
                .lock()
                .unwrap()
                .iter()
                .any(|action| matches!(action, Action::Drop(_)))
        );
        drop(slow_read);
        drop(slow);
        assert!(matches!(
            reap_driver(&mut slow_parent).await,
            Err(ApplicationBodyError::Cancelled)
        ));
        pending.await.unwrap();
        assert_eq!(slow_cleanup.reaped(), 1);
        assert_dropped(&fast_actions);
        assert_dropped(&slow_actions);
    });
}
