/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves an accepted HTTP/1.1 connection on the experimental owned driver.

use std::future::Future;
use std::rc::Rc;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h1::OwnedH1Session;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexTransport;

use super::RunFuture;
use super::handler_application::HandlerApplication;
use super::handler_application::failure;
use super::owned_run::OwnedDriver;
use super::owned_run::OwnedSession;
use super::owned_run::RunFinished;

/// One owned HTTP/1.1 connection and the worker cleanup that retires it.
pub(crate) type OwnedHttp1Session<A> = OwnedSession<OwnedH1Session<A>>;

impl<A: OwnedApplication> OwnedDriver for OwnedH1Session<A> {
    type Handle = OwnedH1Handle;

    const PANIC_SITE: &'static str = "owned_http1_run";

    fn cancel(handle: &OwnedH1Handle) {
        handle.cancel();
    }

    fn run(self) -> RunFuture {
        Box::pin(OwnedH1Session::run(self))
    }
}

impl OwnedHttp1Session<HandlerApplication> {
    /// A session that serves `handler`, whose handler contexts the cleanup finalizes once every
    /// one is released.
    pub(crate) fn new(
        context: &WorkerContext,
        transport: Rc<dyn UniplexTransport>,
        handler: HttpHandlerRef,
        connection: &ConnectionContext,
        io: UniplexConfig,
        http1: Http1DriverConfig,
    ) -> Result<(Self, OwnedH1Handle, RunFinished), HttpError> {
        let mut owner = KeepAliveOwner::new_for_worker((), context.handle())
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let base = HandlerSessionContext::acquire_with_peer_identity(
            &owner,
            connection.peer_identity().map(Rc::from),
        )
        .map_err(|error| failure(error.to_string().as_bytes()))?;
        Self::new_with(
            context,
            OwnedH1Options {
                context: context.clone(),
                transport,
                io,
                http1,
            },
            async move { owner.finalize().await },
            |_| HandlerApplication::new(handler, base),
        )
    }
}

impl<A: OwnedApplication> OwnedHttp1Session<A> {
    /// A session whose application `make` builds from its handle. The cleanup runs `finalize`
    /// once the run has finished.
    pub(crate) fn new_with(
        context: &WorkerContext,
        options: OwnedH1Options,
        finalize: impl Future<Output = ()> + 'static,
        make: impl FnOnce(&OwnedH1Handle) -> A,
    ) -> Result<(Self, OwnedH1Handle, RunFinished), HttpError> {
        Self::with_cleanup(context, finalize, |_| {
            OwnedH1Session::new_with(options, make)
        })
    }
}
