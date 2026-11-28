/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves an accepted HTTP/2 connection on the experimental owned driver.

use std::future::Future;
use std::rc::Rc;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned_h2::OwnedH2Handle;
use roxygen_session::owned_h2::OwnedH2Options;
use roxygen_session::owned_h2::OwnedH2Session;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::LifecycleObserverId;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexTransport;

use super::RunFuture;
use super::handler_application::HandlerApplication;
use super::handler_application::failure;
use super::owned_run::OwnedDriver;
use super::owned_run::OwnedSession;
use super::owned_run::RunFinished;

/// One owned HTTP/2 connection and the worker cleanup that retires it.
pub(crate) type OwnedHttp2Session<A> = OwnedSession<OwnedH2Session<A>>;

impl<A: OwnedApplication> OwnedDriver for OwnedH2Session<A> {
    type Handle = OwnedH2Handle;

    const PANIC_SITE: &'static str = "owned_http2_run";

    fn cancel(handle: &OwnedH2Handle) {
        handle.cancel();
    }

    fn run(self) -> RunFuture {
        Box::pin(OwnedH2Session::run(self))
    }
}

impl OwnedHttp2Session<HandlerApplication> {
    /// A session that serves `handler`, whose handler contexts the cleanup finalizes once every
    /// one is released.
    pub(crate) fn new(
        context: &WorkerContext,
        transport: Rc<dyn UniplexTransport>,
        handler: HttpHandlerRef,
        connection: &ConnectionContext,
        io: UniplexConfig,
        http2: Http2SessionConfig,
    ) -> Result<(Self, OwnedH2Handle, RunFinished), HttpError> {
        let mut owner = KeepAliveOwner::new_for_worker((), context.handle())
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let base = HandlerSessionContext::acquire_with_peer_identity(
            &owner,
            connection.peer_identity().map(Rc::from),
        )
        .map_err(|error| failure(error.to_string().as_bytes()))?;
        Self::new_with(
            context,
            OwnedH2Options {
                context: context.clone(),
                transport,
                io,
                http2,
            },
            async move { owner.finalize().await },
            |_, _| HandlerApplication::new(handler, base),
        )
    }
}

impl<A: OwnedApplication> OwnedHttp2Session<A> {
    /// A session whose application `make` builds from its handle and its finished flag, which
    /// is set once the run has ended, however it ended. The cleanup runs `finalize` once the
    /// run has finished.
    pub(crate) fn new_with(
        context: &WorkerContext,
        options: OwnedH2Options,
        finalize: impl Future<Output = ()> + 'static,
        make: impl FnOnce(&OwnedH2Handle, &RunFinished) -> A,
    ) -> Result<(Self, OwnedH2Handle, RunFinished), HttpError> {
        Self::with_cleanup(context, finalize, |finished| {
            OwnedH2Session::new_with(options, |handle| make(handle, finished))
        })
    }

    pub(crate) fn add_lifecycle_observer(
        &mut self,
        observer: Rc<dyn LifecycleObserver>,
    ) -> Result<LifecycleObserverId, HttpError> {
        self.session.add_lifecycle_observer(observer)
    }
}
