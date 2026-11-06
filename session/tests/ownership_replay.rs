// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::http::HttpMethod;
use roxygen_core::hybrid_source::HttpHybridSource;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::DeferredReadTimeout;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::WaitStatus;

use crate::exchange::ExchangeGuard;
use crate::exchange::ExchangeOwner;
use crate::exchange::ExchangeTracker;
use crate::stream_state::StreamCallbackEvent;
use crate::stream_state::StreamCallbacks;

const LIVE: usize = 64;
const MAXIMUM: u32 = 65_536;
const RECEIVE_WINDOW: u32 = 65_535;

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future + ?Sized>(mut future: Pin<Box<F>>) -> F::Output {
    match poll_once(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the replay phase requires immediate readiness"),
    }
}

fn hold(source: impl HttpSource + 'static) -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(source))
}

fn owner(callbacks: &Rc<StreamCallbacks>) -> ExchangeOwner {
    let weak = Rc::downgrade(callbacks);
    ExchangeOwner::new(
        Rc::new(move || {
            if let Some(callbacks) = weak.upgrade() {
                callbacks.wake_retirement();
            }
        }),
        true,
    )
}

struct Ingress(Rc<StreamCallbacks>);

impl HttpStreamSourceCallback for Ingress {
    fn bytes_processed(&mut self, id: StreamId, amount: usize, to_acknowledge: usize) {
        self.0.push(StreamCallbackEvent::BytesProcessed {
            id,
            amount,
            to_acknowledge,
        });
    }
    fn window_open(&mut self, id: StreamId) {
        self.0.push(StreamCallbackEvent::WindowOpen(id));
    }
    fn source_complete(&mut self, id: StreamId, error: Option<HttpError>) {
        self.0
            .push(StreamCallbackEvent::SourceComplete { id, error });
    }
}

fn native(
    id: StreamId,
    callbacks: &Rc<StreamCallbacks>,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let source = HttpStreamSource::new_session(
        id,
        Ingress(Rc::clone(callbacks)),
        RECEIVE_WINDOW,
        Duration::ZERO,
    )
    .unwrap();
    let producer = source.producer();
    (hold(source), producer)
}

fn upstream(
    id: StreamId,
    callbacks: &Rc<StreamCallbacks>,
    tracker: &ExchangeTracker,
    duplicate: bool,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    // The single covering fence must take over from an already registered owner.
    let handoff = (!duplicate).then(|| tracker.guard());
    let (source, producer) = native(id, callbacks);
    let source = if duplicate {
        tracker.source(source)
    } else {
        source
    };
    assert!(
        producer.defer_read_timeout(DeferredReadTimeout::DeferUpdates(Duration::from_secs(5),))
    );
    let source = tracker.source(source);
    drop(handoff);
    (source, producer)
}

fn message(response: bool, length: &str) -> HttpMessage {
    let mut message = HttpMessage::default();
    if response {
        message.set_status_code(200).unwrap();
    } else {
        message.set_method(HttpMethod::Post).unwrap();
        message.set_url(b"/", false).unwrap();
    }
    message.set_http_version(2, 0);
    message
        .headers_mut()
        .set(b"content-length", length.as_bytes());
    message
}

struct Graph {
    owners: [Option<ExchangeOwner>; 2],
    trackers: [ExchangeTracker; 2],
    receipts: [ExchangeCompletion; 2],
    producers: [HttpStreamSourceProducer; 2],
    payloads: [Bytes; 2],
    request: Option<HttpSourceHolder>,
    response: Option<HttpSourceHolder>,
    guards: [Option<ExchangeGuard>; 3], // D handler, D transfer, U transfer.
}

impl Graph {
    fn new(
        id: StreamId,
        callbacks: &[Rc<StreamCallbacks>; 2],
        duplicate: bool,
        size: usize,
    ) -> Self {
        let payloads = std::array::from_fn(|_| Bytes::from(vec![0x5a; size]));
        let owners = [owner(&callbacks[0]), owner(&callbacks[1])];
        let trackers = owners.each_ref().map(ExchangeOwner::tracker);
        let receipts = owners.each_ref().map(ExchangeOwner::completion);
        let (request, request_producer) = native(id, &callbacks[0]);
        let request = trackers[0].source(request);
        let (response, response_producer) = upstream(id, &callbacks[1], &trackers[1], duplicate);
        let guards = [
            Some(trackers[0].guard()),
            Some(trackers[0].guard()),
            Some(trackers[1].guard()),
        ];
        Self {
            owners: owners.map(Some),
            trackers,
            receipts,
            producers: [request_producer, response_producer],
            payloads,
            request: Some(request),
            response: Some(response),
            guards,
        }
    }
}

#[derive(Default)]
struct BatchCallbacks {
    // One bit per graph proves exactly-once delivery, without event-log allocations.
    seen: [[u64; 4]; 2], // BytesProcessed, SourceComplete, HandlerComplete, EgressProducerComplete.
    bytes: [usize; 2],
    signalled: [u64; 2],
    header_reads: [usize; 4],
    body_reads: [usize; 2],
}

impl BatchCallbacks {
    fn drain(
        &mut self,
        callbacks: &[Rc<StreamCallbacks>; 2],
        graphs: &mut [Graph],
        duplicate: bool,
    ) {
        for (leg, queue) in callbacks.iter().enumerate() {
            let pending = queue.has_pending();
            let mut wait = Box::pin(queue.wait(CancellationToken::never()));
            let result = poll_once(wait.as_mut());
            assert_eq!(
                result,
                if pending {
                    Poll::Ready(WaitStatus::Signalled)
                } else {
                    Poll::Pending
                }
            );
            self.signalled[leg] += u64::from(pending);
            drop(wait);
            while let Some(event) = queue.pop_front() {
                let (id, kind) = match event {
                    StreamCallbackEvent::BytesProcessed {
                        id,
                        amount,
                        to_acknowledge,
                    } => {
                        assert_eq!(to_acknowledge, 0);
                        self.bytes[leg] += amount;
                        (id, 0)
                    }
                    StreamCallbackEvent::SourceComplete { id, error } => {
                        assert!(error.is_none());
                        (id, 1)
                    }
                    StreamCallbackEvent::HandlerComplete {
                        id,
                        generation,
                        result,
                        ..
                    } => {
                        assert_eq!((leg, generation), (0, 1));
                        let graph = &mut graphs[usize::try_from(id.get() / 2).unwrap()];
                        assert!(graph.response.is_none());
                        let source = result.unwrap();
                        let source = if duplicate {
                            graph.trackers[0].source(source)
                        } else {
                            source
                        };
                        graph.response = Some(hold(source));
                        (id, 2)
                    }
                    StreamCallbackEvent::EgressProducerComplete {
                        id,
                        generation,
                        error,
                    } => {
                        assert_eq!(generation, 1);
                        assert!(error.is_none());
                        (id, 3)
                    }
                    StreamCallbackEvent::WindowOpen(_) => {
                        panic!("this replay never closes the window")
                    }
                };
                assert!(id.get() % 2 == 1 && id.get() < 2 * LIVE as u64);
                let bit = 1_u64 << (id.get() / 2);
                assert_eq!(self.seen[leg][kind] & bit, 0);
                self.seen[leg][kind] |= bit;
            }
            queue.acknowledge_retirement();
            assert!(!queue.has_pending());
        }
    }
}

fn body(source: &mut HttpSourceHolder, bytes: usize, eom: bool) {
    let event: HttpBodyEvent = ready(source.read_body_event(MAXIMUM)).unwrap();
    assert_eq!(event.eom(), eom);
    let body = event
        .as_body()
        .expect("DATA, including terminal empty DATA");
    assert_eq!(body.chain_length(), bytes);
    assert!(
        body.segments()
            .all(|segment| segment.iter().all(|&byte| byte == 0x5a))
    );
}

fn batch(
    duplicate: bool,
    size: usize,
    length: &str,
    split: bool,
    callbacks: &[Rc<StreamCallbacks>; 2],
) -> BatchCallbacks {
    let mut graphs: Vec<_> = (0..LIVE)
        .map(|i| Graph::new(StreamId::new(2 * i as u64 + 1), callbacks, duplicate, size))
        .collect();
    let mut observed = BatchCallbacks::default();
    for graph in &mut graphs {
        assert!(graph.producers[0].headers(message(false, length), false));
    }
    for graph in &mut graphs {
        let mut header = ready(graph.request.as_mut().unwrap().read_header_event()).unwrap();
        assert!(!header.eom());
        assert_eq!(
            header.headers_mut().method().unwrap(),
            Some(HttpMethod::Post)
        );
        graph.request = Some(hold(HttpHybridSource::from_header_event(
            header,
            graph.request.take().unwrap(),
        )));
    }
    observed.header_reads[0] += graphs.len();
    for graph in &mut graphs {
        let mut source = graph.request.take().unwrap();
        let mut header = ready(source.read_header_event()).unwrap();
        assert!(!header.eom());
        assert_eq!(
            header.headers_mut().method().unwrap(),
            Some(HttpMethod::Post)
        );
        let source = graph.trackers[1].source(source);
        let source = if duplicate {
            graph.trackers[1].source(source)
        } else {
            source
        };
        graph.request = Some(hold(source));
    }
    observed.header_reads[1] += graphs.len();
    observed.drain(callbacks, &mut graphs, duplicate);
    for leg in 0..2 {
        if leg == 1 {
            for (i, graph) in graphs.iter_mut().enumerate() {
                assert!(graph.producers[1].headers(message(true, length), false));
                let mut source = graph.response.take().unwrap();
                let header = ready(source.read_header_event()).unwrap();
                assert!(!header.eom());
                let source = graph.trackers[0]
                    .source(hold(HttpHybridSource::from_header_event(header, source)));
                callbacks[0].push_handler_complete(StreamCallbackEvent::HandlerComplete {
                    id: StreamId::new(2 * i as u64 + 1),
                    generation: 1,
                    result: Ok(source),
                    proposal: None,
                });
                drop(graph.guards[0].take());
            }
            observed.header_reads[2] += graphs.len();
            observed.drain(callbacks, &mut graphs, duplicate);
            for graph in &mut graphs {
                let mut header =
                    ready(graph.response.as_mut().unwrap().read_header_event()).unwrap();
                assert!(!header.eom());
                assert_eq!(header.headers_mut().status_code().unwrap(), 200);
            }
            observed.header_reads[3] += graphs.len();
            observed.drain(callbacks, &mut graphs, duplicate);
        }
        for graph in &graphs {
            assert_eq!(
                graph.producers[leg].body(
                    BufQueue::from_bytes(graph.payloads[leg].clone(), size).unwrap(),
                    0,
                    !split
                ),
                FlowControlState::Open
            );
        }
        for graph in &mut graphs {
            let source = if leg == 0 {
                &mut graph.request
            } else {
                &mut graph.response
            };
            body(source.as_mut().unwrap(), size, !split);
        }
        observed.body_reads[leg] += graphs.len();
        observed.drain(callbacks, &mut graphs, duplicate);
        if split {
            for graph in &graphs {
                assert!(graph.producers[leg].eom());
            }
            for graph in &mut graphs {
                let source = if leg == 0 {
                    &mut graph.request
                } else {
                    &mut graph.response
                };
                body(source.as_mut().unwrap(), 0, true);
            }
            observed.body_reads[leg] += graphs.len();
            observed.drain(callbacks, &mut graphs, duplicate);
        }
        for (i, graph) in graphs.iter_mut().enumerate() {
            callbacks[1 - leg].push(StreamCallbackEvent::EgressProducerComplete {
                id: StreamId::new(2 * i as u64 + 1),
                generation: 1,
                error: None,
            });
            if leg == 0 {
                assert!(graph.producers[1].arm_read_timeout());
            }
        }
        observed.drain(callbacks, &mut graphs, duplicate);
    }
    for graph in &mut graphs {
        drop(graph.request.take());
        drop(graph.response.take());
        for tracker in &graph.trackers {
            tracker.mark_detached();
        }
        for owner in &graph.owners {
            assert!(!owner.as_ref().unwrap().ready());
        }
        for guard in &mut graph.guards {
            drop(guard.take());
        }
    }
    observed.drain(callbacks, &mut graphs, duplicate);
    assert_eq!(
        observed.seen,
        [[u64::MAX; 4], [u64::MAX, u64::MAX, 0, u64::MAX]]
    );
    assert_eq!(observed.bytes, [LIVE * size; 2]);
    assert_eq!(observed.header_reads, [LIVE; 4]);
    assert_eq!(observed.body_reads, [LIVE * (1 + usize::from(split)); 2]);
    for mut graph in graphs {
        for leg in 0..2 {
            assert!(!graph.owners[leg].as_ref().unwrap().ready());
            assert!(graph.trackers[leg].claim_driver_update());
            graph.trackers[leg].finish_driver();
            assert!(graph.owners[leg].as_ref().unwrap().ready());
            graph.owners[leg].take().unwrap().publish();
            assert_eq!(ready(graph.receipts[leg].wait()), Ok(()));
            assert!(!graph.producers[leg].is_live());
        }
    }
    observed
}

#[test]
fn ownership_replay_validates_batches_and_retained_reads() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        for duplicate in [true, false] {
            for size in [128, 16_384] {
                for split in [false, true] {
                    let queues = [
                        Rc::new(StreamCallbacks::new()),
                        Rc::new(StreamCallbacks::new()),
                    ];
                    batch(duplicate, size, &size.to_string(), split, &queues);
                }
            }
            let queues = [
                Rc::new(StreamCallbacks::new()),
                Rc::new(StreamCallbacks::new()),
            ];
            let up = owner(&queues[1]);
            let tracker = up.tracker();
            let receipt = up.completion();
            let (mut source, _) = upstream(StreamId::new(1), &queues[1], &tracker, duplicate);
            let mut read = source.read_header_event();
            assert!(poll_once(read.as_mut()).is_pending());
            drop(source);
            tracker.mark_detached();
            tracker.finish_driver();
            assert!(!up.ready());
            assert!(poll_once(receipt.wait().as_mut()).is_pending());
            drop(read);
            assert!(up.ready());
            up.publish();
            assert_eq!(ready(receipt.wait()), Ok(()));

            let down = owner(&queues[0]);
            let up = owner(&queues[1]);
            let (mut source, producer) =
                upstream(StreamId::new(1), &queues[1], &up.tracker(), duplicate);
            assert!(producer.headers(message(true, "128"), false));
            let header = ready(source.read_header_event()).unwrap();
            let source = down
                .tracker()
                .source(hold(HttpHybridSource::from_header_event(header, source)));
            let source = if duplicate {
                down.tracker().source(source)
            } else {
                source
            };
            let mut source = hold(source);
            let read = source.read_header_event();
            drop(source);
            for tracker in [down.tracker(), up.tracker()] {
                tracker.mark_detached();
                tracker.finish_driver();
            }
            let down_receipt = down.completion();
            let up_receipt = up.completion();
            assert!(!down.ready());
            assert!(up.ready());
            up.publish();
            assert_eq!(ready(up_receipt.wait()), Ok(()));
            assert!(poll_once(down_receipt.wait().as_mut()).is_pending());
            drop(read);
            assert!(down.ready());
            down.publish();
            assert_eq!(ready(down_receipt.wait()), Ok(()));

            let down = owner(&queues[0]);
            let up = owner(&queues[1]);
            let (mut source, producer) =
                upstream(StreamId::new(1), &queues[1], &up.tracker(), duplicate);
            assert!(producer.headers(message(true, "128"), false));
            let header = ready(source.read_header_event()).unwrap();
            let source = down
                .tracker()
                .source(hold(HttpHybridSource::from_header_event(header, source)));
            let source = if duplicate {
                down.tracker().source(source)
            } else {
                source
            };
            let mut source = hold(source);
            assert!(!ready(source.read_header_event()).unwrap().eom());
            // This owned read retains guards for both exchanges without polling.
            // Polling an empty native body can return a separate Suspend lease.
            let read = source.read_body_event(MAXIMUM);
            drop(source);
            let receipts = [down.completion(), up.completion()];
            for owner in [&down, &up] {
                owner.tracker().mark_detached();
                owner.tracker().finish_driver();
                assert!(!owner.ready());
            }
            for receipt in &receipts {
                assert!(poll_once(receipt.wait().as_mut()).is_pending());
            }
            assert!(producer.is_live());
            drop(read);
            assert!(!producer.is_live());
            for owner in [down, up] {
                assert!(owner.ready());
                owner.publish();
            }
            for receipt in receipts {
                assert_eq!(ready(receipt.wait()), Ok(()));
            }
        }
    });
}

fn cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let mut fields = stat.rsplit_once(')').unwrap().1.split_whitespace();
    let user: u64 = fields.nth(11).unwrap().parse().unwrap();
    let system: u64 = fields.next().unwrap().parse().unwrap();
    user.checked_add(system).unwrap()
}

#[test]
#[ignore]
fn measure_ownership_replay() {
    let env = |suffix: &str, default: &str| {
        std::env::var(format!("ROXYGEN_OWNERSHIP_REPLAY_{suffix}"))
            .unwrap_or_else(|_| default.to_owned())
    };
    let arm = env("ARM", "duplicate");
    let duplicate = match arm.as_str() {
        "duplicate" => true,
        "single" => false,
        _ => panic!("invalid arm"),
    };
    let size: usize = env("PAYLOAD", "128").parse().unwrap();
    assert!(matches!(size, 128 | 16_384));
    let eom = env("EOM", "data");
    let split = match eom.as_str() {
        "data" => false,
        "split" => true,
        _ => panic!("invalid EOM case"),
    };
    let warmup: u64 = env("WARMUP_SECONDS", "1").parse().unwrap();
    let measure: u64 = env("MEASURE_SECONDS", "3").parse().unwrap();
    assert!((1..=60).contains(&warmup) && (1..=60).contains(&measure));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let queues = [Rc::new(StreamCallbacks::new()), Rc::new(StreamCallbacks::new())];
        let length = size.to_string();
        let started = Instant::now();
        let mut warm_batches = 0_u64;
        while started.elapsed() < Duration::from_secs(warmup) {
            batch(duplicate, size, &length, split, &queues);
            warm_batches += 1;
        }
        let warm_actual = started.elapsed();
        let cpu_before = cpu_ticks();
        let started = Instant::now();
        let mut batches = 0_u64;
        let mut signalled = [0_u64; 2];
        let mut events = [[0_u64; 4]; 2];
        let mut bytes = [0_u64; 2];
        let mut header_reads = [0_u64; 4];
        let mut body_reads = [0_u64; 2];
        while started.elapsed() < Duration::from_secs(measure) {
            let observed = batch(duplicate, size, &length, split, &queues);
            for leg in 0..2 {
                signalled[leg] += observed.signalled[leg];
                bytes[leg] += observed.bytes[leg] as u64;
                body_reads[leg] += observed.body_reads[leg] as u64;
                for (total, seen) in events[leg].iter_mut().zip(observed.seen[leg]) {
                    *total += u64::from(seen.count_ones());
                }
            }
            for (total, reads) in header_reads.iter_mut().zip(observed.header_reads) {
                *total += reads as u64;
            }
            batches += 1;
        }
        let measure_actual = started.elapsed();
        let cpu_after = cpu_ticks();
        let requests = batches * LIVE as u64;
        let reads = 1 + u64::from(split);
        let source_owners = if duplicate { [3, 4] } else { [2, 2] };
        let read_guards = if duplicate { [3 + 3 * reads, 2 + 4 * reads] } else { [2 + 2 * reads, 1 + 2 * reads] };
        println!(concat!("\nOWNERSHIP_REPLAY_JSON {{\"kind\":\"roxygen_ownership_replay\",\"version\":1,",
            "\"arm\":\"{}\",\"payload_bytes\":{},\"eom\":\"{}\",\"live_graphs\":64,\"requests\":{},\"errors\":0,",
            "\"warmup_requests\":{},\"warmup_seconds\":{},\"measurement_seconds\":{},",
            "\"requested_warmup_seconds\":{},\"requested_measurement_seconds\":{},",
            "\"cpu_before_ticks\":{},\"cpu_after_ticks\":{},\"cpu_ticks\":{},",
            "\"derived\":{{\"source_owners_per_request_du\":{:?},\"read_guards_per_request_du\":{:?},",
            "\"temporary_handoff_guards_per_request_u\":{},\"body_reads_per_request_per_leg\":{}}},",
            "\"observed\":{{\"callback_event_order\":[\"bytes_processed\",\"source_complete\",\"handler_complete\",\"egress_producer_complete\"],",
            "\"callback_events_du\":{:?},\"bytes_processed_du\":{:?},\"signalled_phase_waits_du\":{:?},\"header_reads_request_original_request_replay_response_original_response_replay\":{:?},\"body_reads_request_response\":{:?}}},\"validation\":\"passed\"}}"),
            arm, size, eom, requests, warm_batches * LIVE as u64, warm_actual.as_secs_f64(), measure_actual.as_secs_f64(),
            warmup, measure, cpu_before, cpu_after, cpu_after - cpu_before, source_owners, read_guards, u64::from(!duplicate), reads,
            events, bytes, signalled, header_reads, body_reads);
    });
}
