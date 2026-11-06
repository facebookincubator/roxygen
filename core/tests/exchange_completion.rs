use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use roxygen_runtime::keep_alive::KeepAliveOwner;

use crate::connection::ConnectionContext;
use crate::connection::TransportEndpoint;
use crate::connection::TransportSecurity;
use crate::exchange::ExchangeCompletion;
use crate::exchange::ExchangeCompletionError;
use crate::exchange::ExchangeCompletionObserver;
use crate::exchange::LocalExchangeCompletionFuture;
use crate::handler::HandlerSessionContext;

struct Observation {
    calls: Rc<Cell<usize>>,
    receiver: tokio::sync::watch::Receiver<Option<Result<(), ExchangeCompletionError>>>,
}

impl ExchangeCompletionObserver for Observation {
    fn wait(&self) -> LocalExchangeCompletionFuture {
        self.calls.set(self.calls.get() + 1);
        let mut receiver = self.receiver.clone();
        Box::pin(async move {
            loop {
                if let Some(result) = *receiver.borrow_and_update() {
                    return result;
                }
                if receiver.changed().await.is_err() {
                    return Err(ExchangeCompletionError::CleanupUnconfirmed);
                }
            }
        })
    }
}

#[test]
fn completion_observation_contract() {
    let (publisher, receiver) = tokio::sync::watch::channel(None);
    let calls = Rc::new(Cell::new(0));
    let receipt = ExchangeCompletion::new(Rc::new(Observation {
        calls: Rc::clone(&calls),
        receiver,
    }));
    let mut owner = KeepAliveOwner::new(());
    let connection = Arc::new(ConnectionContext::new(
        TransportEndpoint::UnnamedUnix,
        TransportEndpoint::UnnamedUnix,
        TransportSecurity::Plaintext,
        None,
    ));
    let context = HandlerSessionContext::acquire(&owner)
        .unwrap()
        .with_exchange_completion(receipt.clone())
        .unwrap()
        .with_connection_context(Arc::clone(&connection))
        .unwrap();
    assert!(Arc::ptr_eq(
        context.connection_context().unwrap(),
        &connection
    ));
    let cloned = context.clone();
    let mut waiting = cloned.exchange_completion().unwrap().wait();
    let mut task_context = Context::from_waker(Waker::noop());
    assert!(waiting.as_mut().poll(&mut task_context).is_pending());
    drop(waiting);
    assert_eq!(calls.get(), 1);
    let (lost_publisher, lost_receiver) = tokio::sync::watch::channel(None);
    let lost = ExchangeCompletion::new(Rc::new(Observation {
        calls: Rc::clone(&calls),
        receiver: lost_receiver,
    }));
    assert!(cloned.with_exchange_completion(lost.clone()).is_err());
    drop(lost_publisher);
    assert_eq!(
        lost.wait().as_mut().poll(&mut task_context),
        Poll::Ready(Err(ExchangeCompletionError::CleanupUnconfirmed)),
    );
    publisher.send_replace(Some(Ok(())));
    drop(publisher);
    assert_eq!(
        receipt.wait().as_mut().poll(&mut task_context),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        context
            .exchange_completion()
            .unwrap()
            .wait()
            .as_mut()
            .poll(&mut task_context),
        Poll::Ready(Ok(()))
    );
    drop(context);
    assert!(
        Box::pin(owner.finalize())
            .as_mut()
            .poll(&mut task_context)
            .is_ready()
    );
}
