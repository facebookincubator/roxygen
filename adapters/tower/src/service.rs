use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tower_service::Service;

/// An owner-local future returned by a worker-local service.
///
/// This future deliberately has no `Send` bound. Its state is created, polled,
/// cancelled, and destroyed on the selected Roxygen worker.
pub type LocalServiceFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// A request handler whose mutable state and futures remain on one worker.
pub trait LocalService<Request>: 'static {
    type Response: Send + 'static;
    type Error: Error + Send + Sync + 'static;
    type Future: Future<Output = Result<Self::Response, Self::Error>> + 'static;

    fn call(&mut self, request: Request) -> Self::Future;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ServiceCreateError {
    #[error("Tower service command capacity must be non-zero")]
    ZeroCapacity,
    #[error(transparent)]
    Worker(#[from] SubmitError),
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError<E>
where
    E: Error + 'static,
{
    #[error(transparent)]
    Handler(#[from] E),
    #[error("worker-local Tower service is closed")]
    Closed,
}

struct ServiceCommand<Request, Response, E> {
    request: Request,
    response: oneshot::Sender<Result<Response, E>>,
    cancellation: oneshot::Receiver<()>,
    _admission: tokio::sync::OwnedSemaphorePermit,
}

type ReservationFuture = Pin<
    Box<
        dyn Future<Output = Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError>>
            + Send
            + 'static,
    >,
>;

enum Readiness {
    Idle,
    Reserving(ReservationFuture),
    Ready(tokio::sync::OwnedSemaphorePermit),
    Closed,
}

struct AdmissionCloseGuard {
    admission: Arc<tokio::sync::Semaphore>,
}

impl Drop for AdmissionCloseGuard {
    fn drop(&mut self) {
        self.admission.close();
    }
}

/// A `Send` Tower facade for a service whose state is owned by one worker.
///
/// `poll_ready` owns one real permit from the bounded admission semaphore that
/// gates the worker command channel. Repeated readiness polls retain that
/// permit, `call` transfers it to the command until completion or cancellation,
/// and dropping the facade releases an unconsumed permit. Each returned call
/// future owns a one-shot cancellation signal for its admitted command. Worker
/// receiver destruction closes admission and wakes pending readiness even when
/// another clone still owns the final permit.
pub struct RoxygenService<Request, Response, E> {
    sender: mpsc::Sender<ServiceCommand<Request, Response, E>>,
    admission: Arc<tokio::sync::Semaphore>,
    readiness: Readiness,
}

impl<Request, Response, E> Clone for RoxygenService<Request, Response, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            admission: Arc::clone(&self.admission),
            readiness: Readiness::Idle,
        }
    }
}

impl<Request, Response, E> RoxygenService<Request, Response, E>
where
    Request: Send + 'static,
    Response: Send + 'static,
    E: Error + Send + Sync + 'static,
{
    pub fn new<S, F>(
        worker: &WorkerHandle,
        command_capacity: usize,
        factory: F,
    ) -> Result<Self, ServiceCreateError>
    where
        S: LocalService<Request, Response = Response, Error = E>,
        F: FnOnce(&WorkerContext) -> S + Send + 'static,
    {
        if command_capacity == 0 {
            return Err(ServiceCreateError::ZeroCapacity);
        }

        let (sender, receiver) = mpsc::channel(command_capacity);
        let admission = Arc::new(tokio::sync::Semaphore::new(command_capacity));
        let admission_close = AdmissionCloseGuard {
            admission: Arc::clone(&admission),
        };
        worker.spawn_local(move |context| async move {
            let _admission_close = admission_close;
            run_local_service(receiver, factory(&context)).await;
        })?;
        Ok(Self {
            sender,
            admission,
            readiness: Readiness::Idle,
        })
    }
}

impl<Request, Response, E> Service<Request> for RoxygenService<Request, Response, E>
where
    Request: Send + 'static,
    Response: Send + 'static,
    E: Error + Send + Sync + 'static,
{
    type Response = Response;
    type Error = ServiceError<E>;
    type Future = CallFuture<Response, E>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.readiness {
                Readiness::Idle => {
                    if self.sender.is_closed() || self.admission.is_closed() {
                        self.readiness = Readiness::Closed;
                        return Poll::Ready(Err(ServiceError::Closed));
                    }
                    self.readiness =
                        Readiness::Reserving(Box::pin(Arc::clone(&self.admission).acquire_owned()));
                }
                Readiness::Reserving(reservation) => match reservation.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(permit)) => {
                        if self.sender.is_closed() || self.admission.is_closed() {
                            self.readiness = Readiness::Closed;
                            return Poll::Ready(Err(ServiceError::Closed));
                        }
                        self.readiness = Readiness::Ready(permit);
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(_closed)) => {
                        self.readiness = Readiness::Closed;
                        return Poll::Ready(Err(ServiceError::Closed));
                    }
                },
                Readiness::Ready(_) => {
                    if self.sender.is_closed() || self.admission.is_closed() {
                        self.readiness = Readiness::Closed;
                        return Poll::Ready(Err(ServiceError::Closed));
                    }
                    return Poll::Ready(Ok(()));
                }
                Readiness::Closed => return Poll::Ready(Err(ServiceError::Closed)),
            }
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let readiness = std::mem::replace(&mut self.readiness, Readiness::Idle);
        let permit = match readiness {
            Readiness::Ready(permit) => permit,
            other => {
                self.readiness = other;
                panic!("RoxygenService::call requires a successful poll_ready reservation");
            }
        };
        let (response, receiver) = oneshot::channel();
        let (cancel, cancellation) = oneshot::channel();
        let command = ServiceCommand {
            request,
            response,
            cancellation,
            _admission: permit,
        };
        if let Err(error) = self.sender.try_send(command) {
            drop(error.into_inner());
        }
        CallFuture {
            response: Some(receiver),
            cancel: Some(cancel),
        }
    }
}

/// The `Send` result future returned by [`RoxygenService::call`].
pub struct CallFuture<Response, E> {
    response: Option<oneshot::Receiver<Result<Response, E>>>,
    cancel: Option<oneshot::Sender<()>>,
}

impl<Response, E> Future for CallFuture<Response, E>
where
    E: Error + 'static,
{
    type Output = Result<Response, ServiceError<E>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = self
            .response
            .as_mut()
            .expect("a completed Tower call future cannot be polled again");
        match Pin::new(receiver).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.response = None;
                self.cancel = None;
                Poll::Ready(match result {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(error)) => Err(ServiceError::Handler(error)),
                    Err(_closed) => Err(ServiceError::Closed),
                })
            }
        }
    }
}

impl<Response, E> Drop for CallFuture<Response, E> {
    fn drop(&mut self) {
        if self.response.is_some() {
            if let Some(cancel) = self.cancel.take() {
                let _result = cancel.send(());
            }
        }
    }
}

async fn run_local_service<Request, Response, E, S>(
    mut receiver: mpsc::Receiver<ServiceCommand<Request, Response, E>>,
    mut service: S,
) where
    Request: Send + 'static,
    Response: Send + 'static,
    E: Error + Send + Sync + 'static,
    S: LocalService<Request, Response = Response, Error = E>,
{
    while let Some(command) = receiver.recv().await {
        run_local_call(&mut service, command).await;
    }
}

async fn run_local_call<Request, Response, E, S>(
    service: &mut S,
    command: ServiceCommand<Request, Response, E>,
) where
    Request: Send + 'static,
    Response: Send + 'static,
    E: Error + Send + Sync + 'static,
    S: LocalService<Request, Response = Response, Error = E>,
{
    let ServiceCommand {
        request,
        response,
        mut cancellation,
        _admission,
    } = command;
    match cancellation.try_recv() {
        Ok(()) | Err(oneshot::error::TryRecvError::Closed) => return,
        Err(oneshot::error::TryRecvError::Empty) => {}
    }

    let mut call = Box::pin(service.call(request));
    let result = std::future::poll_fn(|context| {
        if Pin::new(&mut cancellation).poll(context).is_ready() {
            return Poll::Ready(None);
        }
        call.as_mut().poll(context).map(Some)
    })
    .await;
    if let Some(result) = result {
        let _result = response.send(result);
    }
}
