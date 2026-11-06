use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::filter_chain::FilterChain;
use crate::ids::StreamId;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_filter::FilterMutationError;
use crate::source_filter::HttpSourceFilter;
use crate::source_filter::HttpSourceFilterHooks;
use crate::source_filter::MutateFilter;
use crate::source_holder::SourceReleaseError;

#[derive(Default)]
struct SourceObservations {
    stops: usize,
    stop_errors: Vec<Option<HttpErrorCode>>,
    drops: usize,
    body_maximums: Vec<u32>,
    timeouts: Vec<Duration>,
}

struct ScriptedSource {
    observations: Rc<RefCell<SourceObservations>>,
    headers: VecDeque<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
    bodies: VecDeque<Result<HttpBodyEvent, HttpError>>,
    stream_id: Option<StreamId>,
    on_header_factory: Option<Box<dyn Fn()>>,
    on_stop: Option<Box<dyn Fn()>>,
    on_drop: Option<Box<dyn Fn()>>,
    on_stream_id: Option<Box<dyn Fn()>>,
    on_timeout: Option<Box<dyn Fn()>>,
}

impl ScriptedSource {
    fn new(observations: Rc<RefCell<SourceObservations>>) -> Self {
        Self {
            observations,
            headers: VecDeque::new(),
            bodies: VecDeque::new(),
            stream_id: None,
            on_header_factory: None,
            on_stop: None,
            on_drop: None,
            on_stream_id: None,
            on_timeout: None,
        }
    }

    fn with_header(mut self, event: Result<HttpHeaderEvent, HttpError>) -> Self {
        self.headers.push_back(Box::pin(std::future::ready(event)));
        self
    }

    fn with_header_future(
        mut self,
        event: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> Self {
        self.headers.push_back(event);
        self
    }

    fn with_body(mut self, event: Result<HttpBodyEvent, HttpError>) -> Self {
        self.bodies.push_back(event);
        self
    }

    fn with_stream_id(mut self, stream_id: StreamId) -> Self {
        self.stream_id = Some(stream_id);
        self
    }

    fn with_header_factory_callback(mut self, callback: impl Fn() + 'static) -> Self {
        self.on_header_factory = Some(Box::new(callback));
        self
    }

    fn with_stop_callback(mut self, callback: impl Fn() + 'static) -> Self {
        self.on_stop = Some(Box::new(callback));
        self
    }

    fn with_drop_callback(mut self, callback: impl Fn() + 'static) -> Self {
        self.on_drop = Some(Box::new(callback));
        self
    }

    fn with_stream_id_callback(mut self, callback: impl Fn() + 'static) -> Self {
        self.on_stream_id = Some(Box::new(callback));
        self
    }

    fn with_timeout_callback(mut self, callback: impl Fn() + 'static) -> Self {
        self.on_timeout = Some(Box::new(callback));
        self
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if let Some(callback) = &self.on_header_factory {
            callback();
        }
        self.headers.pop_front().unwrap_or_else(|| {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"header script exhausted",
            ))))
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.observations.borrow_mut().body_maximums.push(maximum);
        let event = self.bodies.pop_front().unwrap_or_else(|| {
            Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"body script exhausted",
            ))
        });
        Box::pin(std::future::ready(event))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        let mut observations = self.observations.borrow_mut();
        observations.stops += 1;
        observations.stop_errors.push(error);
        drop(observations);
        if let Some(callback) = &self.on_stop {
            callback();
        }
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        if let Some(callback) = &self.on_stream_id {
            callback();
        }
        self.stream_id
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.observations.borrow_mut().timeouts.push(timeout);
        if let Some(callback) = &self.on_timeout {
            callback();
        }
    }
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        self.observations.borrow_mut().drops += 1;
        if let Some(callback) = &self.on_drop {
            callback();
        }
    }
}

struct NameHooks {
    name: &'static str,
    effects: Rc<RefCell<Vec<&'static str>>>,
    drops: Option<Rc<Cell<usize>>>,
}

impl NameHooks {
    fn new(name: &'static str, effects: Rc<RefCell<Vec<&'static str>>>) -> Self {
        Self {
            name,
            effects,
            drops: None,
        }
    }

    fn with_drops(mut self, drops: Rc<Cell<usize>>) -> Self {
        self.drops = Some(drops);
        self
    }
}

impl HttpSourceFilterHooks for NameHooks {
    fn filter_name(&self) -> &'static str {
        "TestName"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let name = self.name;
        let effects = Rc::clone(&self.effects);
        Box::pin(async move {
            let result = downstream.await;
            if result.is_ok() {
                effects.borrow_mut().push(name);
            }
            result
        })
    }

    fn read_body_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let name = self.name;
        let effects = Rc::clone(&self.effects);
        Box::pin(async move {
            let result = downstream.await;
            if result.is_ok() {
                effects.borrow_mut().push(name);
            }
            result
        })
    }
}

impl Drop for NameHooks {
    fn drop(&mut self) {
        if let Some(drops) = &self.drops {
            drops.set(drops.get() + 1);
        }
    }
}

struct FactoryProbeHooks {
    probe: Rc<RefCell<Option<Rc<dyn Fn() -> bool>>>>,
    observed: Rc<Cell<bool>>,
}

impl HttpSourceFilterHooks for FactoryProbeHooks {
    fn filter_name(&self) -> &'static str {
        "FactoryProbe"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let readable = self
            .probe
            .borrow()
            .as_ref()
            .expect("the probe is installed before the read")();
        self.observed.set(readable);
        downstream
    }
}

fn request(path: &[u8]) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method("GET").unwrap();
    message.set_url(path, false).unwrap();
    message
}

fn response(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message
}

fn header(message: HttpMessage, eom: bool) -> HttpHeaderEvent {
    HttpHeaderEvent::new(message, eom).unwrap()
}

fn body(bytes: &[u8], eom: bool) -> HttpBodyEvent {
    let queue = BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap();
    HttpBodyEvent::body(queue, eom)
}

fn name_filter(name: &'static str, effects: Rc<RefCell<Vec<&'static str>>>) -> HttpSourceFilter {
    HttpSourceFilter::new(NameHooks::new(name, effects))
}

fn ready<T>(future: LocalSourceFuture<T>) -> T {
    let mut future = future;
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test source unexpectedly returned a pending future"),
    }
}

fn poll_once<T>(future: &mut LocalSourceFuture<T>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Future::poll(future.as_mut(), &mut context)
}

struct ManualCompletion<T> {
    result: Rc<RefCell<Option<T>>>,
}

impl<T> ManualCompletion<T> {
    fn complete(&self, result: T) {
        assert!(self.result.borrow_mut().replace(result).is_none());
    }
}

struct ManualFuture<T> {
    result: Rc<RefCell<Option<T>>>,
}

impl<T> Future for ManualFuture<T> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.result
            .borrow_mut()
            .take()
            .map_or(Poll::Pending, Poll::Ready)
    }
}

fn manual_future<T: 'static>() -> (ManualCompletion<T>, LocalSourceFuture<T>) {
    let result = Rc::new(RefCell::new(None));
    (
        ManualCompletion {
            result: Rc::clone(&result),
        },
        Box::pin(ManualFuture { result }),
    )
}

mod filter_chain {
    use super::*;

    #[test]
    fn set_source_before_insert_preserves_effect_order() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_header(Ok(header(request(b"/filterchain"), true)));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();

        assert!(chain.head().is_none());
        assert!(chain.set_source(Box::new(source)).is_ok());
        assert!(chain.head().is_some());
        chain
            .insert_front(name_filter("filter1", Rc::clone(&effects)))
            .unwrap();
        chain
            .insert_end(name_filter("filter2", Rc::clone(&effects)))
            .unwrap();

        let mut event = ready(chain.head().unwrap().read_header_event()).unwrap();
        assert_eq!(&*effects.borrow(), &["filter2", "filter1"]);
        assert_eq!(event.headers_mut().path().unwrap(), b"/filterchain");
        assert!(!chain.readable());
        assert_eq!(observations.borrow().stops, 0);
    }

    #[test]
    fn release_transfers_ownership() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_header(Ok(header(request(b"/filterchain"), true)));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();
        assert!(chain.set_source(Box::new(source)).is_ok());
        chain
            .insert_front(name_filter("filter1", Rc::clone(&effects)))
            .unwrap();
        chain
            .insert_end(name_filter("filter2", Rc::clone(&effects)))
            .unwrap();

        let mut released = chain.release().unwrap().unwrap();
        assert!(chain.head().is_none());
        assert_eq!(observations.borrow().stops, 0);
        let mut event = ready(released.read_header_event()).unwrap();
        assert_eq!(event.headers_mut().path().unwrap(), b"/filterchain");
        assert_eq!(&*effects.borrow(), &["filter2", "filter1"]);
        assert_eq!(observations.borrow().stops, 0);
    }

    #[test]
    fn insert_before_source_preserves_effect_order() {
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();
        chain
            .insert_end(name_filter("filter1", Rc::clone(&effects)))
            .unwrap();
        assert!(chain.head().is_none());
        chain
            .insert_front(name_filter("filter2", Rc::clone(&effects)))
            .unwrap();
        assert!(chain.head().is_none());

        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        assert!(
            chain
                .set_source(Box::new(
                    ScriptedSource::new(observations)
                        .with_header(Ok(header(request(b"/filterchain"), true))),
                ))
                .is_ok()
        );
        let mut event = ready(chain.head().unwrap().read_header_event()).unwrap();
        assert_eq!(&*effects.borrow(), &["filter1", "filter2"]);
        assert_eq!(event.headers_mut().path().unwrap(), b"/filterchain");
    }

    #[test]
    fn drop_stops_entire_unread_chain_once() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let filter_drops = Rc::new(Cell::new(0));
        let effects = Rc::new(RefCell::new(Vec::new()));
        {
            let source = ScriptedSource::new(Rc::clone(&observations))
                .with_header(Ok(header(response(200), false)))
                .with_body(Ok(body(b"hello world", true)));
            let mut chain = FilterChain::new();
            chain
                .insert_end(HttpSourceFilter::new(
                    NameHooks::new("filter1", Rc::clone(&effects))
                        .with_drops(Rc::clone(&filter_drops)),
                ))
                .unwrap();
            chain
                .insert_front(HttpSourceFilter::new(
                    NameHooks::new("filter2", Rc::clone(&effects))
                        .with_drops(Rc::clone(&filter_drops)),
                ))
                .unwrap();
            assert!(chain.set_source(Box::new(source)).is_ok());

            let mut event = ready(chain.head().unwrap().read_header_event()).unwrap();
            assert_eq!(event.headers_mut().status_code(), Ok(200));
            assert_eq!(&*effects.borrow(), &["filter1", "filter2"]);
            assert!(chain.head().is_some());
        }
        let observations = observations.borrow();
        assert_eq!(observations.stops, 1);
        assert_eq!(observations.stop_errors, &[None]);
        assert_eq!(observations.drops, 1);
        assert_eq!(filter_drops.get(), 2);
    }

    #[test]
    fn replacement_stops_only_the_replaced_source_and_keeps_filters() {
        let first = Rc::new(RefCell::new(SourceObservations::default()));
        let second = Rc::new(RefCell::new(SourceObservations::default()));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();
        chain
            .insert_front(name_filter("filter", Rc::clone(&effects)))
            .unwrap();
        assert!(
            chain
                .set_source(Box::new(
                    ScriptedSource::new(Rc::clone(&first))
                        .with_header(Ok(header(request(b"/old"), true))),
                ))
                .is_ok()
        );
        assert!(
            chain
                .set_source(Box::new(
                    ScriptedSource::new(Rc::clone(&second))
                        .with_header(Ok(header(request(b"/new"), true))),
                ))
                .is_ok()
        );

        assert_eq!(first.borrow().stops, 1);
        assert_eq!(first.borrow().drops, 1);
        let mut event = ready(chain.head().unwrap().read_header_event()).unwrap();
        assert_eq!(event.headers_mut().path().unwrap(), b"/new");
        assert_eq!(&*effects.borrow(), &["filter"]);
        assert_eq!(second.borrow().stops, 0);
    }

    #[test]
    fn explicit_stop_forwards_the_error_once_through_the_whole_chain() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let filter_drops = Rc::new(Cell::new(0));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let source = ScriptedSource::new(Rc::clone(&observations));
        let mut chain = FilterChain::new();
        assert!(chain.set_source(Box::new(source)).is_ok());
        chain
            .insert_front(HttpSourceFilter::new(
                NameHooks::new("front", Rc::clone(&effects)).with_drops(Rc::clone(&filter_drops)),
            ))
            .unwrap();
        chain
            .insert_end(HttpSourceFilter::new(
                NameHooks::new("end", effects).with_drops(Rc::clone(&filter_drops)),
            ))
            .unwrap();

        chain
            .head()
            .unwrap()
            .stop_reading(Some(HttpErrorCode::CANCEL));
        assert!(!chain.readable());
        assert_eq!(observations.borrow().stops, 1);
        assert_eq!(
            observations.borrow().stop_errors,
            &[Some(HttpErrorCode::CANCEL)]
        );
        assert_eq!(observations.borrow().drops, 1);
        assert_eq!(filter_drops.get(), 2);
        drop(chain);
        assert_eq!(observations.borrow().stops, 1);
    }

    #[test]
    fn stream_id_timeout_and_body_maximum_forward_through_every_filter() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_stream_id(StreamId::new(17))
            .with_body(Ok(body(b"abc", true)));
        let mut chain = FilterChain::new();
        assert!(chain.set_source(Box::new(source)).is_ok());
        chain
            .insert_front(name_filter("front", Rc::clone(&effects)))
            .unwrap();
        chain
            .insert_end(name_filter("end", Rc::clone(&effects)))
            .unwrap();

        let head = chain.head().unwrap();
        assert_eq!(head.get_stream_id(), Some(StreamId::new(17)));
        head.set_read_timeout(Duration::from_millis(45));
        let event = ready(head.read_body_event(2)).unwrap();
        assert!(event.eom());
        assert_eq!(&*effects.borrow(), &["end", "front"]);
        let observations = observations.borrow();
        assert_eq!(observations.body_maximums, &[2]);
        assert_eq!(observations.timeouts, &[Duration::from_millis(45)]);
    }

    #[test]
    fn active_read_rejects_release_without_changing_chain_ownership() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let pending = Box::pin(std::future::pending());
        let source = ScriptedSource::new(Rc::clone(&observations)).with_header_future(pending);
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();
        assert!(chain.set_source(Box::new(source)).is_ok());
        chain.insert_front(name_filter("filter", effects)).unwrap();

        let mut read = chain.head().unwrap().read_header_event();
        assert!(poll_once(&mut read).is_pending());
        assert!(matches!(
            chain.release(),
            Err(SourceReleaseError::ReadInProgress)
        ));
        assert!(chain.readable());
        drop(read);
        assert!(chain.release().unwrap().is_some());
        assert!(!chain.readable());
    }

    #[test]
    fn pending_read_rejects_replacement_and_stale_terminal_cannot_consume_it() {
        let first = Rc::new(RefCell::new(SourceObservations::default()));
        let second = Rc::new(RefCell::new(SourceObservations::default()));
        let (completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&first)).with_header_future(pending);
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut chain = FilterChain::new();
        assert!(chain.set_source(Box::new(source)).is_ok());
        chain
            .insert_front(name_filter("filter", Rc::clone(&effects)))
            .unwrap();

        let mut read = chain.head().unwrap().read_header_event();
        assert!(poll_once(&mut read).is_pending());
        let replacement = ScriptedSource::new(Rc::clone(&second))
            .with_header(Ok(header(request(b"/replacement"), true)));
        let rejection = match chain.set_source(Box::new(replacement)) {
            Ok(()) => panic!("replacement during a pending read must be rejected"),
            Err(rejection) => rejection,
        };
        assert_eq!(rejection.reason(), FilterMutationError::ReadInProgress);
        let replacement = rejection.into_source();
        assert!(chain.readable());
        assert_eq!(first.borrow().stops, 0);
        assert_eq!(second.borrow().drops, 0);

        completion.complete(Ok(header(request(b"/original"), true)));
        let mut original = match poll_once(&mut read) {
            Poll::Ready(Ok(event)) => event,
            Poll::Ready(Err(_)) => panic!("the original read must retain its result"),
            Poll::Pending => panic!("the completed original read remained pending"),
        };
        assert_eq!(original.headers_mut().path().unwrap(), b"/original");
        assert_eq!(&*effects.borrow(), &["filter"]);
        assert!(!chain.readable());
        assert_eq!(first.borrow().stops, 0);
        assert_eq!(first.borrow().drops, 1);
        assert_eq!(second.borrow().drops, 0);

        let mut replacement = crate::source_holder::HttpSourceHolder::new(replacement);
        let mut replacement_event = ready(replacement.read_header_event()).unwrap();
        assert_eq!(
            replacement_event.headers_mut().path().unwrap(),
            b"/replacement"
        );
        assert!(!replacement.readable());
        assert_eq!(second.borrow().stops, 0);
        assert_eq!(second.borrow().drops, 1);
    }

    #[test]
    fn replacement_and_stop_callbacks_observe_committed_filter_state() {
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut filter = name_filter("filter", effects);
        let probe: Rc<dyn Fn() -> bool> = Rc::from(filter.readable_probe());
        let observations = Rc::new(RefCell::new(Vec::new()));

        let stop_probe = Rc::clone(&probe);
        let stop_observations = Rc::clone(&observations);
        let drop_probe = Rc::clone(&probe);
        let drop_observations = Rc::clone(&observations);
        let first_counts = Rc::new(RefCell::new(SourceObservations::default()));
        let first = ScriptedSource::new(Rc::clone(&first_counts))
            .with_stop_callback(move || {
                stop_observations
                    .borrow_mut()
                    .push(("replace-stop", stop_probe()));
            })
            .with_drop_callback(move || {
                drop_observations
                    .borrow_mut()
                    .push(("replace-drop", drop_probe()));
            });
        assert!(filter.set_source(Box::new(first)).is_ok());

        let stop_probe = Rc::clone(&probe);
        let stop_observations = Rc::clone(&observations);
        let drop_probe = Rc::clone(&probe);
        let drop_observations = Rc::clone(&observations);
        let second_counts = Rc::new(RefCell::new(SourceObservations::default()));
        let second = ScriptedSource::new(Rc::clone(&second_counts))
            .with_stop_callback(move || {
                stop_observations
                    .borrow_mut()
                    .push(("explicit-stop", stop_probe()));
            })
            .with_drop_callback(move || {
                drop_observations
                    .borrow_mut()
                    .push(("explicit-drop", drop_probe()));
            });
        assert!(filter.set_source(Box::new(second)).is_ok());
        assert_eq!(
            &*observations.borrow(),
            &[("replace-stop", true), ("replace-drop", true)]
        );
        assert_eq!(first_counts.borrow().stops, 1);
        assert_eq!(first_counts.borrow().drops, 1);

        filter.stop_reading(Some(HttpErrorCode::CANCEL));
        assert_eq!(
            &*observations.borrow(),
            &[
                ("replace-stop", true),
                ("replace-drop", true),
                ("explicit-stop", false),
                ("explicit-drop", false),
            ]
        );
        assert_eq!(second_counts.borrow().stops, 1);
        assert_eq!(second_counts.borrow().drops, 1);
    }

    #[test]
    fn forwarding_callbacks_observe_a_readable_filter_without_borrow_panics() {
        let effects = Rc::new(RefCell::new(Vec::new()));
        let mut filter = name_filter("filter", effects);
        let probe: Rc<dyn Fn() -> bool> = Rc::from(filter.readable_probe());
        let observations = Rc::new(RefCell::new(Vec::new()));

        let header_probe = Rc::clone(&probe);
        let header_observations = Rc::clone(&observations);
        let stream_probe = Rc::clone(&probe);
        let stream_observations = Rc::clone(&observations);
        let timeout_probe = Rc::clone(&probe);
        let timeout_observations = Rc::clone(&observations);
        let source_counts = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(Rc::clone(&source_counts))
            .with_header(Ok(header(request(b"/callback"), true)))
            .with_stream_id(StreamId::new(81))
            .with_header_factory_callback(move || {
                header_observations
                    .borrow_mut()
                    .push(("read", header_probe()));
            })
            .with_stream_id_callback(move || {
                stream_observations
                    .borrow_mut()
                    .push(("stream", stream_probe()));
            })
            .with_timeout_callback(move || {
                timeout_observations
                    .borrow_mut()
                    .push(("timeout", timeout_probe()));
            });
        assert!(filter.set_source(Box::new(source)).is_ok());

        assert_eq!(filter.get_stream_id(), Some(StreamId::new(81)));
        filter.set_read_timeout(Duration::from_millis(9));
        let event = ready(filter.read_header_event()).unwrap();
        assert!(event.eom());
        assert_eq!(
            &*observations.borrow(),
            &[("stream", true), ("timeout", true), ("read", true)]
        );
        assert_eq!(source_counts.borrow().timeouts, [Duration::from_millis(9)]);
    }

    #[test]
    fn hook_factory_can_observe_filter_state_without_a_borrow_panic() {
        let probe = Rc::new(RefCell::new(None));
        let observed = Rc::new(Cell::new(false));
        let mut filter = HttpSourceFilter::new(FactoryProbeHooks {
            probe: Rc::clone(&probe),
            observed: Rc::clone(&observed),
        });
        let readable_probe: Rc<dyn Fn() -> bool> = Rc::from(filter.readable_probe());
        probe.borrow_mut().replace(readable_probe);
        let source_counts = Rc::new(RefCell::new(SourceObservations::default()));
        let source =
            ScriptedSource::new(source_counts).with_header(Ok(header(request(b"/hook"), true)));
        assert!(filter.set_source(Box::new(source)).is_ok());

        let event = ready(filter.read_header_event()).unwrap();
        assert!(event.eom());
        assert!(observed.get());
    }

    #[test]
    fn empty_chain_owns_and_releases_a_source_without_extra_behavior() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let mut chain = FilterChain::new();
        assert!(!chain.readable());
        assert!(
            chain
                .set_source(Box::new(
                    ScriptedSource::new(Rc::clone(&observations))
                        .with_header(Ok(header(request(b"/plain"), true))),
                ))
                .is_ok()
        );
        assert!(chain.readable());
        let mut source = chain.release().unwrap().unwrap();
        let mut event = ready(source.read_header_event()).unwrap();
        assert_eq!(event.headers_mut().path().unwrap(), b"/plain");
        assert_eq!(observations.borrow().stops, 0);
    }

    #[test]
    fn custom_filter_reports_its_exact_name() {
        let effects = Rc::new(RefCell::new(Vec::new()));
        let filter = name_filter("effect", effects);
        assert_eq!(filter.filter_name(), "TestName");
    }
}

mod mutate_filter {
    use super::*;

    #[test]
    fn simple_test() {
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_header(Ok(header(response(200), false)))
            .with_body(Ok(body(b"hello", true)));
        let header_hook = Box::new(|event: &mut HttpHeaderEvent| {
            let headers = event.headers_mut().headers_mut();
            headers.add(b"x-header-a".as_slice(), b"x-value-a");
            headers.add(b"x-header-b".as_slice(), b"x-value-b");
        });
        let body_hook = Box::new(|event: &mut HttpBodyEvent| {
            let body = event
                .as_body_mut()
                .expect("the scripted event is body data");
            assert_eq!(body.front().unwrap().as_ref(), b"hello");
            *body = BufQueue::from_bytes(Bytes::from_static(b"world"), 5).unwrap();
        });
        let mut filter =
            MutateFilter::with_source(Box::new(source), Some(header_hook), Some(body_hook));

        assert_eq!(filter.filter_name(), "Mutate");
        let header = ready(filter.read_header_event()).unwrap();
        assert_eq!(
            header
                .headers()
                .headers()
                .get_single_or_empty(b"x-header-a"),
            b"x-value-a"
        );
        assert_eq!(
            header
                .headers()
                .headers()
                .get_single_or_empty(b"x-header-b"),
            b"x-value-b"
        );
        assert!(!header.eom());

        let body = ready(filter.read_body_event(31)).unwrap();
        assert_eq!(body.as_body().unwrap().front().unwrap().as_ref(), b"world");
        assert!(body.eom());
        let observations = observations.borrow();
        assert_eq!(observations.body_maximums, &[31]);
        assert_eq!(observations.stops, 0);
        assert_eq!(observations.drops, 1);
    }

    #[test]
    fn pass_thru_on_error() {
        let header_calls = Rc::new(Cell::new(0));
        let body_calls = Rc::new(Cell::new(0));
        let expected = HttpError::with_message(HttpErrorCode::CANCEL, b"cancelled");
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(observations).with_header(Err(expected));
        let header_counter = Rc::clone(&header_calls);
        let body_counter = Rc::clone(&body_calls);
        let mut filter = MutateFilter::with_source(
            Box::new(source),
            Some(Box::new(move |_event| {
                header_counter.set(header_counter.get() + 1);
            })),
            Some(Box::new(move |_event| {
                body_counter.set(body_counter.get() + 1);
            })),
        );

        let error = match ready(filter.read_header_event()) {
            Ok(_) => panic!("the scripted source must return its error"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        assert_eq!(error.message(), b"cancelled");
        assert_eq!(header_calls.get(), 0);
        assert_eq!(body_calls.get(), 0);
        assert!(!filter.readable());
    }

    #[test]
    fn body_error_is_unchanged_and_does_not_invoke_hook() {
        let calls = Rc::new(Cell::new(0));
        let observations = Rc::new(RefCell::new(SourceObservations::default()));
        let source = ScriptedSource::new(observations).with_body(Err(HttpError::with_message(
            HttpErrorCode::READ_TIMEOUT,
            b"late body",
        )));
        let hook_calls = Rc::clone(&calls);
        let mut filter = MutateFilter::with_source(
            Box::new(source),
            None,
            Some(Box::new(move |_event| {
                hook_calls.set(hook_calls.get() + 1);
            })),
        );

        let error = match ready(filter.read_body_event(7)) {
            Ok(_) => panic!("the scripted source must return its error"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"late body");
        assert_eq!(calls.get(), 0);
    }
}
