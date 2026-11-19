/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod tower_service {
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::error::Error;
    use std::fmt;
    use std::future::Future;
    use std::num::NonZeroU32;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc as std_mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::thread::ThreadId;
    use std::time::Duration;

    use ::http_body::Body as HttpBody;
    use ::tower_service::Service;
    use axum::Router;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::routing::post;
    use bytes::Bytes;
    use futures::task::noop_waker;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::events::HttpBodyEvent;
    use roxygen_core::events::HttpHeaderEvent;
    use roxygen_core::headers::HttpHeaderName;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::source::HttpSource;
    use roxygen_core::source::LocalSourceFuture;
    use roxygen_hyper_adapter::body::RoxygenBody;
    use roxygen_hyper_adapter::body::admit_untrusted_bytes;
    use roxygen_runtime::tokio_host::ManagedWorker;
    use roxygen_runtime::worker::WorkerHandle;
    use tower::ServiceExt;

    use crate::service::LocalService;
    use crate::service::LocalServiceFuture;
    use crate::service::RoxygenService;
    use crate::service::ServiceError;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ServiceAction {
        Construct {
            label: &'static str,
            owner: ThreadId,
        },
        Call {
            label: &'static str,
            owner: ThreadId,
        },
        FutureDrop {
            label: &'static str,
            owner: ThreadId,
        },
        RequestDrop {
            label: &'static str,
            owner: ThreadId,
        },
        Drop {
            label: &'static str,
            owner: ThreadId,
        },
    }

    fn record_service(actions: &Arc<Mutex<Vec<ServiceAction>>>, action: ServiceAction) {
        actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(action);
    }

    fn service_actions(actions: &Arc<Mutex<Vec<ServiceAction>>>) -> Vec<ServiceAction> {
        actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    struct EchoLocal {
        label: &'static str,
        owner: ThreadId,
        calls: Rc<Cell<usize>>,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
    }

    impl LocalService<Bytes> for EchoLocal {
        type Response = Bytes;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Bytes, TestError>>;

        fn call(&mut self, request: Bytes) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            self.calls.set(self.calls.get() + 1);
            record_service(
                &self.actions,
                ServiceAction::Call {
                    label: self.label,
                    owner,
                },
            );
            let local = Rc::clone(&self.calls);
            Box::pin(async move {
                tokio::task::yield_now().await;
                assert_eq!(std::thread::current().id(), owner);
                assert!(local.get() > 0);
                Ok(request)
            })
        }
    }

    impl Drop for EchoLocal {
        fn drop(&mut self) {
            record_service(
                &self.actions,
                ServiceAction::Drop {
                    label: self.label,
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    fn echo_service(
        worker: &ManagedWorker,
        capacity: usize,
        label: &'static str,
        actions: &Arc<Mutex<Vec<ServiceAction>>>,
    ) -> RoxygenService<Bytes, Bytes, TestError> {
        let owner = worker.owner_thread();
        let local_actions = Arc::clone(actions);
        RoxygenService::new(&worker.handle(), capacity, move |context| {
            assert_eq!(context.owner_thread(), owner);
            record_service(&local_actions, ServiceAction::Construct { label, owner });
            EchoLocal {
                label,
                owner,
                calls: Rc::new(Cell::new(0)),
                actions: local_actions,
            }
        })
        .expect("Tower service starts")
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    fn poll_ready_once<Request, S>(service: &mut S) -> Poll<Result<(), S::Error>>
    where
        S: Service<Request>,
    {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        service.poll_ready(&mut context)
    }

    fn assert_send<T: Send>() {}

    fn assert_send_value<T: Send>(_value: &T) {}

    #[test]
    fn service_is_send_and_session_stays_local() {
        assert_send::<RoxygenService<Bytes, Bytes, TestError>>();

        let caller = std::thread::current().id();
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        assert_ne!(caller, owner);
        let actions = Arc::new(Mutex::new(Vec::new()));
        let service = echo_service(&worker, 1, "send", &actions);

        let request = service.clone().oneshot(Bytes::from_static(b"ping"));
        assert_send_value(&request);
        assert_eq!(
            runtime().block_on(request).expect("worker call succeeds"),
            Bytes::from_static(b"ping"),
        );

        drop(service);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            service_actions(&actions),
            [
                ServiceAction::Construct {
                    label: "send",
                    owner,
                },
                ServiceAction::Call {
                    label: "send",
                    owner,
                },
                ServiceAction::Drop {
                    label: "send",
                    owner,
                },
            ],
        );
    }

    #[test]
    fn poll_ready_reserves_bounded_capacity() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut first = echo_service(&worker, 1, "bounded", &actions);
        let mut second = first.clone();

        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut first),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut first),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut second),
            Poll::Pending,
        ));

        let first_call = first.call(Bytes::from_static(b"first"));
        assert_eq!(
            runtime()
                .block_on(first_call)
                .expect("first admitted call completes"),
            Bytes::from_static(b"first"),
        );
        let second_call = runtime().block_on(async {
            second
                .ready()
                .await
                .expect("capacity returns after the first command is admitted")
                .call(Bytes::from_static(b"second"))
                .await
        });
        assert_eq!(
            second_call.expect("second admitted call completes"),
            Bytes::from_static(b"second"),
        );

        drop(first);
        drop(second);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter(|action| matches!(action, ServiceAction::Call { .. }))
                .count(),
            2,
        );
    }

    #[test]
    fn drop_after_ready_releases_reservation() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut reserved = echo_service(&worker, 1, "service-drop", &actions);
        let mut waiting = reserved.clone();

        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut reserved),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut waiting),
            Poll::Pending,
        ));
        drop(reserved);

        let response = runtime().block_on(async {
            waiting
                .ready()
                .await
                .expect("dropping the ready clone releases its permit")
                .call(Bytes::from_static(b"released"))
                .await
        });
        assert_eq!(
            response.expect("call after released reservation completes"),
            Bytes::from_static(b"released"),
        );

        drop(waiting);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter(|action| matches!(action, ServiceAction::Call { .. }))
                .count(),
            1,
        );
    }

    #[test]
    fn pending_readiness_observes_shutdown_while_another_clone_holds_the_permit() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut holding = echo_service(&worker, 1, "ready-shutdown", &actions);
        let mut waiting = holding.clone();
        runtime()
            .block_on(
                worker
                    .handle()
                    .call(|_| ())
                    .expect("owner barrier admitted"),
            )
            .expect("owner barrier completes");

        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut holding),
            Poll::Ready(Ok(())),
        ));
        let readiness = runtime().block_on(async move {
            let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
            let readiness_task = tokio::spawn(async move {
                let mut pending_tx = Some(pending_tx);
                let mut readiness = Box::pin(waiting.ready());
                std::future::poll_fn(|context| match readiness.as_mut().poll(context) {
                    Poll::Pending => {
                        if let Some(pending_tx) = pending_tx.take() {
                            pending_tx
                                .send(())
                                .expect("report the initial pending readiness poll");
                        }
                        Poll::Pending
                    }
                    Poll::Ready(result) => Poll::Ready(result.map(|_| ())),
                })
                .await
            });
            pending_rx
                .await
                .expect("readiness reaches a real pending poll");
            worker.shutdown().expect("worker shuts down");
            tokio::time::timeout(Duration::from_secs(1), readiness_task)
                .await
                .expect("pending readiness is woken by bounded shutdown")
                .expect("pending readiness task completes")
        });
        assert!(matches!(readiness, Err(ServiceError::Closed)));
        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut holding),
            Poll::Ready(Err(ServiceError::Closed)),
        ));
        assert!(service_actions(&actions).iter().any(|action| matches!(
            action,
            ServiceAction::Drop {
                label: "ready-shutdown",
                owner: actual,
            } if *actual == owner
        )));
    }

    struct FutureDropProbe {
        label: &'static str,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        dropped: Option<std_mpsc::Sender<ThreadId>>,
    }

    impl Drop for FutureDropProbe {
        fn drop(&mut self) {
            let owner = std::thread::current().id();
            record_service(
                &self.actions,
                ServiceAction::FutureDrop {
                    label: self.label,
                    owner,
                },
            );
            self.dropped
                .take()
                .expect("local future drop reports once")
                .send(owner)
                .expect("report local future drop");
        }
    }

    struct ActiveRequest {
        dropped: Option<std_mpsc::Sender<ThreadId>>,
    }

    impl Drop for ActiveRequest {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                dropped
                    .send(std::thread::current().id())
                    .expect("report active request drop");
            }
        }
    }

    struct CancelLocal {
        owner: ThreadId,
        calls: usize,
        started: Option<std_mpsc::Sender<ThreadId>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        dropped: Option<std_mpsc::Sender<ThreadId>>,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<ActiveRequest> for CancelLocal {
        type Response = Bytes;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Bytes, TestError>>;

        fn call(&mut self, request: ActiveRequest) -> Self::Future {
            self.calls += 1;
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            record_service(
                &self.actions,
                ServiceAction::Call {
                    label: "future-drop",
                    owner,
                },
            );
            if self.calls == 1 {
                let started = self.started.take().expect("first call reports start");
                let release = self.release.take().expect("first call owns its gate");
                let dropped = self.dropped.take().expect("first call reports drop");
                let probe = FutureDropProbe {
                    label: "future-drop",
                    actions: Arc::clone(&self.actions),
                    dropped: Some(dropped),
                };
                return Box::pin(async move {
                    let _probe = probe;
                    let _request = request;
                    started.send(owner).expect("report local future start");
                    let _result = release.await;
                    Ok(Bytes::from_static(b"unreachable"))
                });
            }
            drop(request);
            Box::pin(std::future::ready(Ok(Bytes::from_static(b"after-cancel"))))
        }
    }

    #[test]
    fn dropped_call_future_cancels_and_releases() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let local_actions = Arc::clone(&actions);
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = std_mpsc::channel();
        let (request_dropped_tx, request_dropped_rx) = std_mpsc::channel();
        let mut service = RoxygenService::new(&worker.handle(), 1, move |_| CancelLocal {
            owner,
            calls: 0,
            started: Some(started_tx),
            release: Some(release_rx),
            dropped: Some(dropped_tx),
            actions: local_actions,
            _local: Rc::new(()),
        })
        .expect("Tower service starts");

        assert!(matches!(
            poll_ready_once::<ActiveRequest, _>(&mut service),
            Poll::Ready(Ok(())),
        ));
        let call = service.call(ActiveRequest {
            dropped: Some(request_dropped_tx),
        });
        assert_eq!(
            started_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("local future starts"),
            owner,
        );
        assert!(matches!(
            poll_ready_once::<ActiveRequest, _>(&mut service),
            Poll::Pending,
        ));
        drop(call);
        assert_eq!(
            dropped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("local future is cancelled"),
            owner,
        );
        assert_eq!(
            request_dropped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("active request is destroyed"),
            owner,
        );
        assert!(
            release_tx.send(()).is_err(),
            "cancellation drops the owner-local pending future",
        );

        let response = runtime().block_on(async {
            service
                .ready()
                .await
                .expect("cancelled call releases service admission")
                .call(ActiveRequest { dropped: None })
                .await
        });
        assert_eq!(
            response.expect("next call completes"),
            Bytes::from_static(b"after-cancel"),
        );
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter(|action| matches!(action, ServiceAction::FutureDrop { .. }))
                .count(),
            1,
        );

        drop(service);
        worker.shutdown().expect("managed worker shuts down");
    }

    struct QueuedRequest {
        label: &'static str,
        dropped: Option<std_mpsc::Sender<ThreadId>>,
    }

    impl Drop for QueuedRequest {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                dropped
                    .send(std::thread::current().id())
                    .expect("report queued request drop");
            }
        }
    }

    struct QueuedCancelLocal {
        owner: ThreadId,
        started: Option<std_mpsc::Sender<ThreadId>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<QueuedRequest> for QueuedCancelLocal {
        type Response = &'static str;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Self::Response, Self::Error>>;

        fn call(&mut self, request: QueuedRequest) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            let label = request.label;
            record_service(&self.actions, ServiceAction::Call { label, owner });
            if label == "one" {
                let started = self.started.take().expect("first call reports start");
                let release = self.release.take().expect("first call owns its barrier");
                return Box::pin(async move {
                    let _request = request;
                    started.send(owner).expect("report first call start");
                    release.await.expect("release first call");
                    Ok(label)
                });
            }
            Box::pin(async move {
                let _request = request;
                Ok(label)
            })
        }
    }

    #[test]
    fn queued_call_drop_skips_dispatch_and_releases_one_permit() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let local_actions = Arc::clone(&actions);
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = std_mpsc::channel();
        let mut first = RoxygenService::new(&worker.handle(), 2, move |_| QueuedCancelLocal {
            owner,
            started: Some(started_tx),
            release: Some(release_rx),
            actions: local_actions,
            _local: Rc::new(()),
        })
        .expect("Tower service starts");
        let mut second = first.clone();
        let mut third = first.clone();
        let mut fourth = first.clone();
        let mut fifth = first.clone();

        runtime()
            .block_on(first.ready())
            .expect("first call reserves capacity");
        let first_call = first.call(QueuedRequest {
            label: "one",
            dropped: None,
        });
        assert_eq!(
            started_rx.recv().expect("first call reaches its barrier"),
            owner,
        );

        runtime()
            .block_on(second.ready())
            .expect("second call reserves capacity");
        let second_call = second.call(QueuedRequest {
            label: "two",
            dropped: Some(cancelled_tx),
        });
        drop(second_call);
        assert!(matches!(
            poll_ready_once::<QueuedRequest, _>(&mut third),
            Poll::Pending,
        ));

        release_tx.send(()).expect("release active first call");
        assert_eq!(
            runtime()
                .block_on(first_call)
                .expect("first call completes normally"),
            "one",
        );
        assert_eq!(
            cancelled_rx
                .recv()
                .expect("worker observes queued cancellation"),
            owner,
        );
        runtime().block_on(async {
            third
                .ready()
                .await
                .expect("first returned permit can be reserved");
        });
        assert!(matches!(
            poll_ready_once::<QueuedRequest, _>(&mut fourth),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(
            poll_ready_once::<QueuedRequest, _>(&mut fifth),
            Poll::Pending,
        ));
        drop(fourth);
        let third_response = runtime().block_on(third.call(QueuedRequest {
            label: "three",
            dropped: None,
        }));
        assert_eq!(third_response.expect("third call completes"), "three");
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter_map(|action| match action {
                    ServiceAction::Call { label, .. } => Some(*label),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            ["one", "three"],
            "the cancelled queued request never reaches LocalService::call",
        );

        drop(first);
        drop(second);
        drop(third);
        drop(fifth);
        worker.shutdown().expect("managed worker shuts down");
    }

    struct ShutdownRequest {
        label: &'static str,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
    }

    impl Drop for ShutdownRequest {
        fn drop(&mut self) {
            record_service(
                &self.actions,
                ServiceAction::RequestDrop {
                    label: self.label,
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    struct ShutdownLocal {
        owner: ThreadId,
        started: Option<std_mpsc::Sender<ThreadId>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<ShutdownRequest> for ShutdownLocal {
        type Response = ();
        type Error = TestError;
        type Future = LocalServiceFuture<Result<(), TestError>>;

        fn call(&mut self, request: ShutdownRequest) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            record_service(
                &self.actions,
                ServiceAction::Call {
                    label: request.label,
                    owner,
                },
            );
            let started = self
                .started
                .take()
                .expect("only the active call dispatches");
            let release = self.release.take().expect("active call owns its barrier");
            Box::pin(async move {
                let _request = request;
                started.send(owner).expect("report active call start");
                release
                    .await
                    .expect("test does not release before shutdown");
                Ok(())
            })
        }
    }

    impl Drop for ShutdownLocal {
        fn drop(&mut self) {
            record_service(
                &self.actions,
                ServiceAction::Drop {
                    label: "shutdown-service",
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    #[test]
    fn worker_shutdown_closes_active_and_queued_calls_on_owner() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let local_actions = Arc::clone(&actions);
        let request_actions = Arc::clone(&actions);
        let (started_tx, started_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut active = RoxygenService::new(&worker.handle(), 2, move |_| ShutdownLocal {
            owner,
            started: Some(started_tx),
            release: Some(release_rx),
            actions: local_actions,
            _local: Rc::new(()),
        })
        .expect("Tower service starts");
        let mut queued = active.clone();

        runtime()
            .block_on(active.ready())
            .expect("active call reserves capacity");
        let active_call = active.call(ShutdownRequest {
            label: "active",
            actions: Arc::clone(&request_actions),
        });
        assert_eq!(
            started_rx.recv().expect("active call reaches barrier"),
            owner
        );
        runtime()
            .block_on(queued.ready())
            .expect("queued call reserves capacity");
        let queued_call = queued.call(ShutdownRequest {
            label: "queued",
            actions: Arc::clone(&request_actions),
        });

        worker
            .shutdown()
            .expect("worker cancels and joins local work");
        assert!(
            release_tx.send(()).is_err(),
            "active local future was dropped",
        );
        assert!(matches!(
            runtime().block_on(active_call),
            Err(ServiceError::Closed),
        ));
        assert!(matches!(
            runtime().block_on(queued_call),
            Err(ServiceError::Closed),
        ));
        assert!(matches!(
            poll_ready_once::<ShutdownRequest, _>(&mut active),
            Poll::Ready(Err(ServiceError::Closed)),
        ));

        let actions = service_actions(&actions);
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, ServiceAction::Call { .. }))
                .count(),
            1,
            "the queued call is destroyed without dispatch",
        );
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, ServiceAction::RequestDrop { .. }))
                .count(),
            2,
        );
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(
                    action,
                    ServiceAction::Drop {
                        label: "shutdown-service",
                        owner: actual,
                    } if *actual == owner
                ))
                .count(),
            1,
            "worker shutdown destroys the local service exactly once on its owner",
        );
        assert!(actions.iter().all(|action| match action {
            ServiceAction::Construct { owner: actual, .. }
            | ServiceAction::Call { owner: actual, .. }
            | ServiceAction::FutureDrop { owner: actual, .. }
            | ServiceAction::RequestDrop { owner: actual, .. }
            | ServiceAction::Drop { owner: actual, .. } => *actual == owner,
        }));
        drop(active);
        drop(queued);
    }

    #[test]
    fn call_without_ready_panics_without_dispatch_or_capacity_change() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut service = echo_service(&worker, 1, "without-ready", &actions);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _call = service.call(Bytes::from_static(b"not-admitted"));
        }));
        assert!(result.is_err());
        assert!(
            !service_actions(&actions)
                .iter()
                .any(|action| matches!(action, ServiceAction::Call { .. }))
        );
        assert!(matches!(
            poll_ready_once::<Bytes, _>(&mut service),
            Poll::Ready(Ok(())),
        ));
        assert_eq!(
            runtime()
                .block_on(service.call(Bytes::from_static(b"admitted")))
                .expect("valid call completes after the rejected call"),
            Bytes::from_static(b"admitted"),
        );

        drop(service);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter(|action| matches!(action, ServiceAction::Call { .. }))
                .count(),
            1,
        );
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum SourceAction {
        Construct {
            label: &'static str,
            owner: ThreadId,
        },
        Read {
            label: &'static str,
            owner: ThreadId,
        },
        Stop {
            label: &'static str,
            owner: ThreadId,
            error: Option<HttpErrorCode>,
        },
        Drop {
            label: &'static str,
            owner: ThreadId,
        },
    }

    fn record_source(actions: &Arc<Mutex<Vec<SourceAction>>>, action: SourceAction) {
        actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(action);
    }

    enum ScriptEvent {
        Data {
            segments: Vec<&'static [u8]>,
            terminal: bool,
        },
        Trailers(Vec<(&'static [u8], &'static [u8])>),
    }

    struct ScriptedSource {
        label: &'static str,
        events: VecDeque<ScriptEvent>,
        actions: Arc<Mutex<Vec<SourceAction>>>,
        _local: Rc<()>,
    }

    impl ScriptedSource {
        fn new(
            label: &'static str,
            events: Vec<ScriptEvent>,
            actions: Arc<Mutex<Vec<SourceAction>>>,
        ) -> Self {
            record_source(
                &actions,
                SourceAction::Construct {
                    label,
                    owner: std::thread::current().id(),
                },
            );
            Self {
                label,
                events: events.into(),
                actions,
                _local: Rc::new(()),
            }
        }
    }

    impl HttpSource for ScriptedSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"body-only scripted source",
            ))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            record_source(
                &self.actions,
                SourceAction::Read {
                    label: self.label,
                    owner: std::thread::current().id(),
                },
            );
            let event = self.events.pop_front().expect("scripted event exists");
            Box::pin(std::future::ready(match event {
                ScriptEvent::Data { segments, terminal } => {
                    let length = segments.iter().map(|segment| segment.len()).sum();
                    let mut body = BufQueue::new(length);
                    for segment in segments {
                        body.push_bytes(Bytes::from_static(segment))
                            .expect("scripted data fits its exact bound");
                    }
                    Ok(HttpBodyEvent::body(body, terminal))
                }
                ScriptEvent::Trailers(entries) => {
                    let mut trailers = HttpHeaders::new();
                    trailers.add_all(
                        entries
                            .into_iter()
                            .map(|(name, value)| (HttpHeaderName::from(name), value)),
                    );
                    Ok(HttpBodyEvent::trailers(trailers))
                }
            }))
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            record_source(
                &self.actions,
                SourceAction::Stop {
                    label: self.label,
                    owner: std::thread::current().id(),
                    error,
                },
            );
        }

        fn set_read_timeout(&mut self, _timeout: Duration) {}
    }

    impl Drop for ScriptedSource {
        fn drop(&mut self) {
            record_source(
                &self.actions,
                SourceAction::Drop {
                    label: self.label,
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    fn copied_bytes(bytes: Bytes, maximum: usize) -> Result<Bytes, TestError> {
        admit_untrusted_bytes(bytes, maximum)
            .and_then(|queue| queue.coalesce(maximum))
            .map_err(|_| TestError("request body exceeds its adapter bound"))
    }

    struct ClientLocal {
        owner: ThreadId,
        worker: WorkerHandle,
        service_actions: Arc<Mutex<Vec<ServiceAction>>>,
        source_actions: Arc<Mutex<Vec<SourceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<Request<Bytes>> for ClientLocal {
        type Response = Response<RoxygenBody>;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Self::Response, Self::Error>>;

        fn call(&mut self, request: Request<Bytes>) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            record_service(
                &self.service_actions,
                ServiceAction::Call {
                    label: "client",
                    owner,
                },
            );
            let body = copied_bytes(request.into_body(), 16).and_then(|body| {
                if body != Bytes::from_static(b"ping") {
                    return Err(TestError("unexpected client request"));
                }
                let source_actions = Arc::clone(&self.source_actions);
                RoxygenBody::new(
                    &self.worker,
                    1,
                    NonZeroU32::new(8).expect("nonzero read bound"),
                    move |_| {
                        Box::new(ScriptedSource::new(
                            "client",
                            vec![ScriptEvent::Data {
                                segments: vec![b"pong"],
                                terminal: true,
                            }],
                            source_actions,
                        ))
                    },
                )
                .map_err(|_| TestError("response source could not start"))
            });
            Box::pin(std::future::ready(body.map(Response::new)))
        }
    }

    impl Drop for ClientLocal {
        fn drop(&mut self) {
            record_service(
                &self.service_actions,
                ServiceAction::Drop {
                    label: "client",
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    #[test]
    fn client_service_is_send_with_local_session() {
        type Client = RoxygenService<Request<Bytes>, Response<RoxygenBody>, TestError>;
        assert_send::<Client>();

        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let recorded_service_actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::new(Mutex::new(Vec::new()));
        let local_service_actions = Arc::clone(&recorded_service_actions);
        let local_source_actions = Arc::clone(&source_actions);
        let worker_handle = worker.handle();
        let local_worker = worker_handle.clone();
        let service = Client::new(&worker_handle, 1, move |_| {
            record_service(
                &local_service_actions,
                ServiceAction::Construct {
                    label: "client",
                    owner,
                },
            );
            ClientLocal {
                owner,
                worker: local_worker,
                service_actions: local_service_actions,
                source_actions: local_source_actions,
                _local: Rc::new(()),
            }
        })
        .expect("client service starts");

        let call = service.oneshot(Request::new(Bytes::from_static(b"ping")));
        assert_send_value(&call);
        let mut response = runtime().block_on(call).expect("client call completes");
        let frame = runtime()
            .block_on(response.body_mut().frame())
            .expect("one response frame")
            .expect("response frame succeeds");
        assert_eq!(
            frame.into_data().expect("response data frame"),
            Bytes::from_static(b"pong"),
        );
        assert!(response.body().is_end_stream());

        drop(response);
        worker.shutdown().expect("managed worker shuts down");
        assert!(
            service_actions(&recorded_service_actions)
                .iter()
                .all(|action| match action {
                    ServiceAction::Construct { owner: actual, .. }
                    | ServiceAction::Call { owner: actual, .. }
                    | ServiceAction::FutureDrop { owner: actual, .. }
                    | ServiceAction::RequestDrop { owner: actual, .. }
                    | ServiceAction::Drop { owner: actual, .. } => *actual == owner,
                })
        );
        assert!(
            source_actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .all(|action| match action {
                    SourceAction::Construct { owner: actual, .. }
                    | SourceAction::Read { owner: actual, .. }
                    | SourceAction::Stop { owner: actual, .. }
                    | SourceAction::Drop { owner: actual, .. } => *actual == owner,
                }),
        );
    }

    struct HttpEchoLocal {
        owner: ThreadId,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<Request<Bytes>> for HttpEchoLocal {
        type Response = Response<Bytes>;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Self::Response, Self::Error>>;

        fn call(&mut self, request: Request<Bytes>) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            record_service(
                &self.actions,
                ServiceAction::Call {
                    label: "axum",
                    owner,
                },
            );
            let body = request.into_body();
            Box::pin(async move {
                tokio::task::yield_now().await;
                assert_eq!(std::thread::current().id(), owner);
                Ok(Response::new(body))
            })
        }
    }

    impl Drop for HttpEchoLocal {
        fn drop(&mut self) {
            record_service(
                &self.actions,
                ServiceAction::Drop {
                    label: "axum",
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    #[derive(Clone)]
    struct AxumEchoState {
        service:
            Arc<tokio::sync::Mutex<RoxygenService<Request<Bytes>, Response<Bytes>, TestError>>>,
    }

    fn error_response(message: &'static str) -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(message))
            .expect("static error response is valid")
    }

    async fn axum_echo_handler(
        State(state): State<AxumEchoState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let bytes = match to_bytes(body, 32).await {
            Ok(bytes) => bytes,
            Err(_) => return error_response("request body read failed"),
        };
        let bytes = match copied_bytes(bytes, 32) {
            Ok(bytes) => bytes,
            Err(_) => return error_response("request body admission failed"),
        };
        let request = Request::from_parts(parts, bytes);
        let call = {
            let mut service = state.service.lock().await;
            let ready = match service.ready().await {
                Ok(ready) => ready,
                Err(_) => return error_response("worker service closed"),
            };
            ready.call(request)
        };
        match call.await {
            Ok(response) => response.map(Body::from),
            Err(_) => error_response("worker service call failed"),
        }
    }

    #[test]
    fn axum_server_handler_keeps_session_local() {
        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let local_actions = Arc::clone(&actions);
        let service = RoxygenService::new(&worker.handle(), 2, move |_| {
            record_service(
                &local_actions,
                ServiceAction::Construct {
                    label: "axum",
                    owner,
                },
            );
            HttpEchoLocal {
                owner,
                actions: local_actions,
                _local: Rc::new(()),
            }
        })
        .expect("server service starts");
        let state = AxumEchoState {
            service: Arc::new(tokio::sync::Mutex::new(service)),
        };
        let app = Router::new()
            .route("/echo", post(axum_echo_handler))
            .with_state(state);

        let response = runtime()
            .block_on(
                app.clone().oneshot(
                    Request::post("/echo")
                        .body(Body::from("axum request"))
                        .expect("request is valid"),
                ),
            )
            .expect("Axum router call succeeds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime()
                .block_on(to_bytes(response.into_body(), 32))
                .expect("Axum response body reads"),
            Bytes::from_static(b"axum request"),
        );

        drop(app);
        worker.shutdown().expect("managed worker shuts down");
        assert!(service_actions(&actions).iter().all(|action| match action {
            ServiceAction::Construct { owner: actual, .. }
            | ServiceAction::Call { owner: actual, .. }
            | ServiceAction::FutureDrop { owner: actual, .. }
            | ServiceAction::RequestDrop { owner: actual, .. }
            | ServiceAction::Drop { owner: actual, .. } => *actual == owner,
        }));
        assert_eq!(
            service_actions(&actions)
                .iter()
                .filter(|action| matches!(action, ServiceAction::Call { .. }))
                .count(),
            1,
        );
    }

    #[derive(Clone, Copy)]
    enum ResponsePlan {
        Full,
        Early,
    }

    struct RouterLocal {
        owner: ThreadId,
        actions: Arc<Mutex<Vec<ServiceAction>>>,
        _local: Rc<()>,
    }

    impl LocalService<Request<Bytes>> for RouterLocal {
        type Response = ResponsePlan;
        type Error = TestError;
        type Future = LocalServiceFuture<Result<Self::Response, Self::Error>>;

        fn call(&mut self, request: Request<Bytes>) -> Self::Future {
            let owner = std::thread::current().id();
            assert_eq!(owner, self.owner);
            record_service(
                &self.actions,
                ServiceAction::Call {
                    label: "router-body",
                    owner,
                },
            );
            let plan = if request.uri().path() == "/full" {
                ResponsePlan::Full
            } else {
                ResponsePlan::Early
            };
            Box::pin(std::future::ready(Ok(plan)))
        }
    }

    impl Drop for RouterLocal {
        fn drop(&mut self) {
            record_service(
                &self.actions,
                ServiceAction::Drop {
                    label: "router-body",
                    owner: std::thread::current().id(),
                },
            );
        }
    }

    #[derive(Clone)]
    struct RouterBodyState {
        service: Arc<tokio::sync::Mutex<RoxygenService<Request<Bytes>, ResponsePlan, TestError>>>,
        worker: WorkerHandle,
        source_actions: Arc<Mutex<Vec<SourceAction>>>,
    }

    async fn router_body_handler(
        State(state): State<RouterBodyState>,
        request: Request<Body>,
    ) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let bytes = match to_bytes(body, 32).await {
            Ok(bytes) => bytes,
            Err(_) => return error_response("request body read failed"),
        };
        let bytes = match copied_bytes(bytes, 32) {
            Ok(bytes) => bytes,
            Err(_) => return error_response("request body admission failed"),
        };
        let request = Request::from_parts(parts, bytes);
        let call = {
            let mut service = state.service.lock().await;
            let ready = match service.ready().await {
                Ok(ready) => ready,
                Err(_) => return error_response("worker service closed"),
            };
            ready.call(request)
        };
        let plan = match call.await {
            Ok(plan) => plan,
            Err(_) => return error_response("worker service call failed"),
        };
        let (label, events) = match plan {
            ResponsePlan::Full => (
                "full",
                vec![
                    ScriptEvent::Data {
                        segments: vec![b"alpha", b"beta"],
                        terminal: false,
                    },
                    ScriptEvent::Trailers(vec![(b"x-result", b"complete")]),
                ],
            ),
            ResponsePlan::Early => (
                "early",
                vec![
                    ScriptEvent::Data {
                        segments: vec![b"first"],
                        terminal: false,
                    },
                    ScriptEvent::Data {
                        segments: vec![b"not-read"],
                        terminal: true,
                    },
                ],
            ),
        };
        let source_actions = Arc::clone(&state.source_actions);
        let body = match RoxygenBody::new(
            &state.worker,
            1,
            NonZeroU32::new(16).expect("nonzero body read bound"),
            move |_| Box::new(ScriptedSource::new(label, events, source_actions)),
        ) {
            Ok(body) => body,
            Err(_) => return error_response("response body source failed"),
        };
        Response::new(Body::new(body))
    }

    #[test]
    fn madrox_router_body_trailers_early_drop_keeps_state_local() {
        let worker = ManagedWorker::new(12).expect("managed worker starts");
        let owner = worker.owner_thread();
        let service_actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::new(Mutex::new(Vec::new()));
        let local_actions = Arc::clone(&service_actions);
        let service = RoxygenService::new(&worker.handle(), 2, move |_| {
            record_service(
                &local_actions,
                ServiceAction::Construct {
                    label: "router-body",
                    owner,
                },
            );
            RouterLocal {
                owner,
                actions: local_actions,
                _local: Rc::new(()),
            }
        })
        .expect("router service starts");
        let app = Router::new()
            .route("/full", post(router_body_handler))
            .route("/early", post(router_body_handler))
            .with_state(RouterBodyState {
                service: Arc::new(tokio::sync::Mutex::new(service)),
                worker: worker.handle(),
                source_actions: Arc::clone(&source_actions),
            });

        let mut full = runtime()
            .block_on(
                app.clone().oneshot(
                    Request::post("/full")
                        .body(Body::from("request"))
                        .expect("full request is valid"),
                ),
            )
            .expect("full route succeeds");
        let first = runtime()
            .block_on(full.body_mut().frame())
            .expect("first full frame")
            .expect("first full frame succeeds")
            .into_data()
            .expect("first full frame is data");
        let second = runtime()
            .block_on(full.body_mut().frame())
            .expect("second full frame")
            .expect("second full frame succeeds")
            .into_data()
            .expect("second full frame is data");
        assert_eq!(first, Bytes::from_static(b"alpha"));
        assert_eq!(second, Bytes::from_static(b"beta"));
        let trailers = runtime()
            .block_on(full.body_mut().frame())
            .expect("trailer frame")
            .expect("trailer frame succeeds")
            .into_trailers()
            .expect("last full frame is trailers");
        assert_eq!(trailers.get("x-result").unwrap(), "complete");
        assert!(runtime().block_on(full.body_mut().frame()).is_none());

        let mut early = runtime()
            .block_on(
                app.clone().oneshot(
                    Request::post("/early")
                        .body(Body::from("request"))
                        .expect("early request is valid"),
                ),
            )
            .expect("early route succeeds");
        assert_eq!(
            runtime()
                .block_on(early.body_mut().frame())
                .expect("first early frame")
                .expect("first early frame succeeds")
                .into_data()
                .expect("first early frame is data"),
            Bytes::from_static(b"first"),
        );
        drop(early);
        drop(full);
        drop(app);
        worker.shutdown().expect("managed worker shuts down");

        let source_actions = source_actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert!(source_actions.iter().all(|action| match action {
            SourceAction::Construct { owner: actual, .. }
            | SourceAction::Read { owner: actual, .. }
            | SourceAction::Stop { owner: actual, .. }
            | SourceAction::Drop { owner: actual, .. } => *actual == owner,
        }));
        assert_eq!(
            source_actions
                .iter()
                .filter(|action| matches!(
                    action,
                    SourceAction::Stop {
                        label: "early",
                        error: None,
                        ..
                    }
                ))
                .count(),
            1,
        );
        assert!(
            !source_actions
                .iter()
                .any(|action| matches!(action, SourceAction::Stop { label: "full", .. }))
        );
        assert_eq!(
            source_actions
                .iter()
                .filter(|action| matches!(action, SourceAction::Drop { .. }))
                .count(),
            2,
        );
    }
}
