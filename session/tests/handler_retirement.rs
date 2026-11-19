/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::task_group::TaskGroup;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tokio::time::timeout;

use super::SessionCore;
use crate::exchange::ExchangeOwner;

fn run_local(test: impl Future<Output = ()> + 'static) {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(LocalSet::new().run_until(test));
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn handler_retirement_is_recovered_before_join_error_payload_destruction() {
    struct PanickingPayload;
    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            panic!("join error payload drop");
        }
    }

    run_local(async {
        for async_join in [false, true] {
            let (worker, _driver) = WorkerDriver::new(32).unwrap();
            let mut session = SessionCore::new(
                TransportDirection::Downstream,
                CodecProtocol::Http11,
                worker,
            )
            .unwrap();
            session.require_driver_cleanup();
            session.acknowledge_ingress_reconciliation();
            assert!(!session.has_pending_driver_work());
            let owner = ExchangeOwner::new(Rc::new(|| {}), false);
            let exchange = owner.tracker();
            let receipt = owner.completion();
            let retirement = exchange.guard();
            exchange.mark_detached();
            session.exchanges.insert(0, (StreamId::new(1), owner));
            let polled = Rc::new(Cell::new(false));
            let task_polled = Rc::clone(&polled);
            let task = session
                .handler_tasks_mut()
                .spawn_local_tracked(move |_| async move {
                    task_polled.set(true);
                    std::panic::panic_any(PanickingPayload);
                })
                .unwrap();
            session.handler_retirements.insert(task, retirement);
            timeout(Duration::from_secs(2), async {
                while !polled.get() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!session.exchanges[&0].1.ready());
            assert!(!session.has_pending_events());
            assert!(session.has_pending_driver_work());
            let panic = catch_unwind(AssertUnwindSafe(|| {
                if async_join {
                    let _ = poll_once(Box::pin(session.join_handler_tasks()).as_mut());
                } else {
                    let _ = session.reap_completed_handler_tasks();
                }
            }))
            .unwrap_err();
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"join error payload drop")
            );
            assert!(
                session
                    .handler_tasks
                    .as_ref()
                    .is_none_or(TaskGroup::is_empty)
            );
            assert!(!session.has_pending_driver_work());
            let (_, owner) = session.exchanges.remove(&0).unwrap();
            assert!(owner.ready());
            owner.publish();
            assert_eq!(
                receipt.wait().await,
                Err(ExchangeCompletionError::CleanupUnconfirmed)
            );
            session.finalize().await.unwrap();
        }
    });
}

#[test]
fn abandoned_session_retires_tasks_before_reporting_its_join_invariant() {
    struct Capture {
        drops: Rc<Cell<usize>>,
        panic: bool,
        owner: std::thread::ThreadId,
    }

    impl Drop for Capture {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner);
            self.drops.set(self.drops.get() + 1);
            if self.panic {
                panic!("abandoned task capture drop");
            }
        }
    }

    run_local(async {
        for (siblings, handler) in [(false, true), (true, false), (true, true)] {
            for panic in [false, true] {
                let (worker, _driver) = WorkerDriver::new(4).unwrap();
                let mut session =
                    SessionCore::new(TransportDirection::Downstream, CodecProtocol::Http2, worker)
                        .unwrap();
                let owner = ExchangeOwner::new(Rc::new(|| {}), false);
                let tracker = owner.tracker();
                let receipt = owner.completion();
                session.exchanges.insert(0, (StreamId::new(1), owner));
                let drops = Rc::new(Cell::new(0));
                let mut cancellations = Vec::new();
                for tracked in [false, true] {
                    if (tracked && !handler) || (!tracked && !siblings) {
                        continue;
                    }
                    let capture = Capture {
                        drops: Rc::clone(&drops),
                        panic,
                        owner: std::thread::current().id(),
                    };
                    let tasks = if tracked {
                        session.handler_tasks_mut()
                    } else {
                        &mut session.tasks_mut().tasks
                    };
                    let task = tasks
                        .spawn_local_tracked(|cancellation| {
                            cancellations.push(cancellation);
                            async move {
                                let _capture = capture;
                                std::future::pending::<()>().await;
                            }
                        })
                        .unwrap();
                    if tracked {
                        session.handler_retirements.insert(task, tracker.guard());
                    }
                }
                let failure = catch_unwind(AssertUnwindSafe(|| drop(session)))
                    .expect_err("abandoning a session must still fail its release invariant");
                let expected = if siblings {
                    "session tasks must be joined"
                } else {
                    "session handler tasks must be joined"
                };
                assert_eq!(failure.downcast_ref::<&str>(), Some(&expected));
                assert_eq!(drops.get(), usize::from(siblings) + usize::from(handler));
                assert!(
                    cancellations
                        .iter()
                        .all(|token| token.is_cancellation_requested())
                );
                assert_eq!(
                    receipt.wait().await,
                    Err(ExchangeCompletionError::CleanupUnconfirmed)
                );
            }
        }
    });
}

#[test]
fn abandoned_session_signals_operation_tokens_before_user_destruction() {
    #[derive(Clone)]
    struct Capture {
        name: &'static str,
        token: Rc<RefCell<Option<CancellationToken>>>,
        observed: Rc<RefCell<Vec<(&'static str, bool, bool)>>>,
        owner: std::thread::ThreadId,
    }

    impl Drop for Capture {
        fn drop(&mut self) {
            self.observed.borrow_mut().push((
                self.name,
                self.token
                    .borrow()
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancellation_requested),
                std::thread::current().id() == self.owner,
            ));
        }
    }

    struct PendingHandler {
        capture: Capture,
        entered: tokio::sync::mpsc::Sender<()>,
    }

    impl HttpHandler for PendingHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let mut capture = self.capture.clone();
            capture.name = "handler future";
            self.entered.try_send(()).unwrap();
            Box::pin(async move {
                let _capture = capture;
                let _context = context;
                let _source = source;
                std::future::pending().await
            })
        }
    }

    run_local(async {
        for start_operations in [false, true] {
            let (worker, _driver) = WorkerDriver::new(4).unwrap();
            let mut session =
                SessionCore::new(TransportDirection::Downstream, CodecProtocol::Http2, worker)
                    .unwrap();
            let observed = Rc::new(RefCell::new(Vec::new()));
            let (entered, mut ready) = tokio::sync::mpsc::channel(3);
            let capture = |name, token| Capture {
                name,
                token,
                observed: Rc::clone(&observed),
                owner: std::thread::current().id(),
            };
            let siblings = Rc::new(RefCell::new(Some(
                session.tasks_mut().sibling_cancellation.token(),
            )));
            let read = capture("read", Rc::clone(&siblings));
            let write = capture("write", siblings);
            let read_entered = entered.clone();
            let write_entered = entered.clone();
            session
                .tasks_mut()
                .start(
                    move |token| {
                        *read.token.borrow_mut() = Some(token);
                        read_entered.try_send(()).unwrap();
                        async move {
                            let _capture = read;
                            std::future::pending::<()>().await;
                        }
                    },
                    move |token| {
                        *write.token.borrow_mut() = Some(token);
                        write_entered.try_send(()).unwrap();
                        async move {
                            let _capture = write;
                            std::future::pending::<()>().await;
                        }
                    },
                )
                .unwrap();
            let id = StreamId::new(1);
            let request = session.begin_incoming_stream(id).unwrap();
            let receipt = request.completion().clone();
            let handler_token = Rc::new(RefCell::new(None));
            session
                .start_request_handler(
                    request,
                    Rc::new(PendingHandler {
                        capture: capture("handler", Rc::clone(&handler_token)),
                        entered,
                    }),
                )
                .unwrap();
            *handler_token.borrow_mut() = session.handler_cancellation_for_test(id);
            if start_operations {
                timeout(Duration::from_secs(2), async {
                    for _ in 0..3 {
                        ready.recv().await.expect("operation started");
                    }
                })
                .await
                .unwrap();
            }
            let failure = catch_unwind(AssertUnwindSafe(|| drop(session))).unwrap_err();
            assert_eq!(
                failure.downcast_ref::<&str>(),
                Some(&"session tasks must be joined")
            );
            let mut expected = vec![
                ("handler", true, true),
                ("read", true, true),
                ("write", true, true),
            ];
            if start_operations {
                expected.insert(1, ("handler future", true, true));
            }
            observed.borrow_mut().sort_unstable();
            assert_eq!(*observed.borrow(), expected);
            assert_eq!(
                receipt.wait().await,
                Err(ExchangeCompletionError::CleanupUnconfirmed)
            );
        }
    });
}

struct HandlerFutureDrop(Rc<Cell<usize>>);

impl Drop for HandlerFutureDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// A handler whose future records entry, then waits until it is dropped.
struct WaitingHandler {
    entered: Rc<Cell<bool>>,
    dropped: Rc<Cell<usize>>,
}

impl HttpHandler for WaitingHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.entered.set(true);
        let dropped = HandlerFutureDrop(Rc::clone(&self.dropped));
        Box::pin(async move {
            let _dropped = dropped;
            let _context = context;
            let _source = source;
            std::future::pending().await
        })
    }
}

/// Starts one waiting handler on a downstream HTTP/1.1 session, reporting the
/// session's task groups before the stream, before the handler, and after it,
/// then finalizes the session and returns how many handler futures dropped.
async fn first_handler_then_finalize() -> ([(bool, bool); 3], usize, usize) {
    let (worker, _driver) = WorkerDriver::new(8).unwrap();
    let mut session = SessionCore::new(
        TransportDirection::Downstream,
        CodecProtocol::Http11,
        worker,
    )
    .unwrap();
    let constructed = session.task_groups_created_for_test();
    let id = StreamId::new(1);
    let request = session.begin_incoming_stream(id).unwrap();
    let with_stream = session.task_groups_created_for_test();
    let entered = Rc::new(Cell::new(false));
    let dropped = Rc::new(Cell::new(0));
    session
        .start_request_handler(
            request,
            Rc::new(WaitingHandler {
                entered: Rc::clone(&entered),
                dropped: Rc::clone(&dropped),
            }),
        )
        .unwrap();
    let with_handler = session.task_groups_created_for_test();
    let pending_handlers = session.pending_handler_task_count();
    timeout(Duration::from_secs(2), async {
        while !entered.get() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dropped.get(), 0);
    session.finalize().await.unwrap();
    (
        [constructed, with_stream, with_handler],
        pending_handlers,
        dropped.get(),
    )
}

#[test]
fn first_handler_creates_the_handler_group_and_finalize_joins_it() {
    run_local(async {
        let ([.., (_, handler_group)], pending_handlers, dropped) =
            first_handler_then_finalize().await;
        assert!(handler_group, "the first handler runs in the handler group");
        assert_eq!(pending_handlers, 1);
        assert_eq!(dropped, 1, "finalize joined the cancelled handler task");
    });
}

#[test]
fn downstream_session_creates_no_task_group_before_its_first_handler() {
    run_local(async {
        let ([constructed, with_stream, with_handler], _, dropped) =
            first_handler_then_finalize().await;
        assert_eq!(constructed, (false, false));
        assert_eq!(
            with_stream,
            (false, false),
            "a stream without a handler creates no group"
        );
        assert_eq!(
            with_handler,
            (false, true),
            "the first handler creates only the handler group"
        );
        assert_eq!(dropped, 1);
    });
}
