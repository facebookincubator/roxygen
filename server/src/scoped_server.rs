//! RAII wrapper that drains and force-stops an HTTP server on drop.

use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::HttpServerError;
use crate::server::ServerConfig;
use crate::server::ServerObserver;

pub struct ScopedHttpServer {
    server: HttpServer,
}

impl ScopedHttpServer {
    pub fn start(
        config: ServerConfig,
        handler_factory: HandlerFactory,
        observer: Option<Arc<dyn ServerObserver>>,
    ) -> Result<Self, HttpServerError> {
        Ok(Self {
            server: HttpServer::start(config, handler_factory, observer)?,
        })
    }

    pub fn address(&self) -> Option<SocketAddr> {
        self.server.address()
    }

    pub fn addresses(&self) -> &[SocketAddr] {
        self.server.addresses()
    }

    pub fn server(&self) -> &HttpServer {
        &self.server
    }
}

impl Deref for ScopedHttpServer {
    type Target = HttpServer;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl Drop for ScopedHttpServer {
    fn drop(&mut self) {
        let _ = self.server.drain();
        let _ = self.server.force_stop();
    }
}
