/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;

use crate::handler::HandlerFuture;
use crate::handler::HandlerSessionContext;
use crate::handler::HttpHandler;
pub use crate::handler::HttpHandlerRef;
use crate::handler::LocalHandlerFuture;
use crate::source_holder::HttpSourceHolder;

pub type HttpChainHandlerRef = Rc<HttpChainHandler>;

/// Implements one chain node with access to its stored successor.
pub trait HttpChainHandlerHooks: 'static {
    fn handle_request(
        &self,
        next_handler: Option<HttpHandlerRef>,
        context: HandlerSessionContext,
        request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture;
}

/// A handler with the link storage used by an [`HttpHandlerChain`].
pub struct HttpChainHandler {
    hooks: Box<dyn HttpChainHandlerHooks>,
    next_handler: RefCell<Option<HttpHandlerRef>>,
}

impl HttpChainHandler {
    pub fn new(hooks: impl HttpChainHandlerHooks) -> Self {
        Self {
            hooks: Box::new(hooks),
            next_handler: RefCell::new(None),
        }
    }

    pub fn get_next_handler(&self) -> Option<HttpHandlerRef> {
        self.next_handler.borrow().clone()
    }

    pub fn set_next_handler(&self, next_handler: Option<HttpHandlerRef>) {
        let previous = self.next_handler.replace(next_handler);
        drop(previous);
    }

    pub fn handle_request(
        &self,
        context: HandlerSessionContext,
        request_source: HttpSourceHolder,
    ) -> HandlerFuture {
        let retained_context = context.clone();
        let inner = self
            .hooks
            .handle_request(self.get_next_handler(), context, request_source);
        HandlerFuture::new(retained_context, inner)
    }
}

impl HttpHandler for HttpChainHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(HttpChainHandler::handle_request(
            self,
            context,
            request_source,
        ))
    }
}

/// A shared handler chain with constant-time front and back insertion.
#[derive(Default)]
pub struct HttpHandlerChain {
    front: Option<HttpChainHandlerRef>,
    back: Option<HttpChainHandlerRef>,
}

impl HttpHandlerChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_front(&self) -> Option<HttpChainHandlerRef> {
        self.front.clone()
    }

    pub fn get_back(&self) -> Option<HttpChainHandlerRef> {
        self.back.clone()
    }

    pub fn insert_front(&mut self, handler: Option<HttpChainHandlerRef>) {
        let Some(handler) = handler else {
            return;
        };

        if let Some(front) = self.front.take() {
            handler.set_next_handler(Some(front));
        }
        self.front = Some(Rc::clone(&handler));

        if self.back.is_none() {
            self.back = Some(handler);
        }
    }

    pub fn insert_back(&mut self, handler: Option<HttpChainHandlerRef>) {
        let Some(handler) = handler else {
            return;
        };

        if let Some(back) = &self.back {
            back.set_next_handler(Some(as_handler_ref(&handler)));
        }
        self.back = Some(Rc::clone(&handler));

        if self.front.is_none() {
            self.front = Some(handler);
        }
    }
}

fn as_handler_ref(handler: &HttpChainHandlerRef) -> HttpHandlerRef {
    handler.clone() as HttpHandlerRef
}
