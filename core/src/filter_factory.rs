use std::rc::Rc;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::filter_chain::FilterChain;
use crate::fixed_source::HttpErrorSource;
use crate::fixed_source::get_error_response;
use crate::handler::HandlerSessionContext;
use crate::handler::LocalHandlerFuture;
use crate::handler_chain::HttpChainHandler;
use crate::handler_chain::HttpChainHandlerHooks;
use crate::handler_chain::HttpHandlerRef;
use crate::source::HttpSource;
use crate::source_filter::HttpSourceFilter;
use crate::source_filter::RejectedFilterSource;
use crate::source_holder::HttpSourceHolder;

pub type FilterPair = (Option<HttpSourceFilter>, Option<HttpSourceFilter>);

/// Creates the request and response filters for one handler invocation.
pub trait FilterFactory: 'static {
    fn make_filters(&self) -> FilterPair;
}

/// Builds per-request source-filter chains around a successor handler.
#[derive(Default)]
pub struct HttpFilterFactoryHandler {
    filter_factories: Vec<Rc<dyn FilterFactory>>,
}

impl HttpFilterFactoryHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one factory. A missing factory is ignored like a null shared
    /// factory in the source API.
    pub fn add_filter_factory(&mut self, factory: Option<Rc<dyn FilterFactory>>) -> &mut Self {
        if let Some(factory) = factory {
            self.filter_factories.push(factory);
        }
        self
    }

    pub fn into_chain_handler(self) -> HttpChainHandler {
        HttpChainHandler::new(self)
    }
}

impl HttpChainHandlerHooks for HttpFilterFactoryHandler {
    fn handle_request(
        &self,
        next_handler: Option<HttpHandlerRef>,
        context: HandlerSessionContext,
        request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let filter_factories = self.filter_factories.clone();
        Box::pin(async move {
            let Some(next_handler) = next_handler else {
                return Err(internal_error(
                    b"HTTP filter factory handler has no successor",
                ));
            };

            let mut request_chain = FilterChain::new();
            let mut response_chain = FilterChain::new();
            transfer_holder_to_chain(request_source, &mut request_chain)?;

            for factory in filter_factories {
                let (request_filter, response_filter) = factory.make_filters();
                if let Some(request_filter) = request_filter {
                    request_chain
                        .insert_front(request_filter)
                        .map_err(|error| internal_error(error.to_string()))?;
                }
                if let Some(response_filter) = response_filter {
                    response_chain
                        .insert_end(response_filter)
                        .map_err(|error| internal_error(error.to_string()))?;
                }
            }

            let filtered_request = release_chain(&mut request_chain)?;
            let handler_result = next_handler.handle_request(context, filtered_request).await;
            let response_source = match handler_result {
                Ok(mut response) if response.readable() => response
                    .release()
                    .map_err(|error| internal_error(error.to_string()))?
                    .ok_or_else(|| {
                        internal_error(b"readable handler response did not own a source")
                    })?,
                Ok(_) => Box::new(
                    get_error_response(500, b"")
                        .map_err(|error| internal_error(error.to_string()))?,
                ),
                Err(error) => Box::new(HttpErrorSource::new(error)),
            };

            response_chain
                .set_source(response_source)
                .map_err(rejected_source_error)?;
            release_chain(&mut response_chain)
        })
    }
}

fn transfer_holder_to_chain(
    mut holder: HttpSourceHolder,
    chain: &mut FilterChain,
) -> Result<(), HttpError> {
    let source = holder
        .release()
        .map_err(|error| internal_error(error.to_string()))?;
    if let Some(source) = source {
        chain.set_source(source).map_err(rejected_source_error)?;
    }
    Ok(())
}

fn release_chain(chain: &mut FilterChain) -> Result<HttpSourceHolder, HttpError> {
    let source = chain
        .release()
        .map_err(|error| internal_error(error.to_string()))?;
    Ok(source.map_or_else(HttpSourceHolder::empty, HttpSourceHolder::new))
}

fn rejected_source_error(rejection: RejectedFilterSource) -> HttpError {
    let reason = rejection.reason();
    stop_source(rejection.into_source());
    internal_error(reason.to_string())
}

fn stop_source(source: Box<dyn HttpSource>) {
    let mut holder = HttpSourceHolder::new(source);
    holder.clear(None);
}

fn internal_error(message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message)
}
