//! A `ByteStreamTransport` implemented outside `roxygen_transport`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::ByteStreamTransport;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::byte_stream::TransportRead;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

const REQUEST: &[u8] = b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 3\r\n\r\nabc";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseKind {
    Open,
    ShutdownWrite,
    Closed,
    Reset,
}

/// Replays scripted reads; a read past the script fails the session.
struct ExternalTransport {
    reads: RefCell<VecDeque<TransportRead>>,
    reads_issued: Cell<usize>,
    written: RefCell<Vec<u8>>,
    close: Cell<CloseKind>,
}

impl ByteStreamTransport for ExternalTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        self.reads_issued.set(self.reads_issued.get() + 1);
        let result = self.reads.borrow_mut().pop_front().ok_or_else(|| {
            HttpError::with_message(
                HttpErrorCode::TRANSPORT_READ_ERROR,
                b"read after the scripted end of stream",
            )
        });
        Box::pin(std::future::ready(result))
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let bytes_written = data.chain_length();
        self.written.borrow_mut().extend(bytes_of(&data));
        Box::pin(std::future::ready(Ok(WriteInfo { bytes_written })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.close.set(CloseKind::ShutdownWrite);
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.close.set(CloseKind::Closed);
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.close.set(CloseKind::Reset);
        Ok(())
    }

    fn local_address(&self, _context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        Ok(SocketAddress::new("127.0.0.1", 1))
    }

    fn peer_address(&self, _context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        Ok(SocketAddress::new("127.0.0.1", 2))
    }

    fn peer_identity(&self, _context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        Ok(None)
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        Err(ByteEventRegistrationError::Unsupported)
    }
}

struct BodyHandler {
    body: Rc<RefCell<Vec<u8>>>,
}

impl HttpHandler for BodyHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let body = Rc::clone(&self.body);
        Box::pin(async move {
            let mut eom = request.read_header_event().await?.eom();
            while !eom {
                let event = request.read_body_event(u32::MAX).await?;
                eom = event.eom();
                match event.into_kind() {
                    HttpBodyEventKind::Body(data) => body.borrow_mut().extend(bytes_of(&data)),
                    HttpBodyEventKind::Suspend(resume) => {
                        let _ = resume.await;
                    }
                    _ => {}
                }
            }
            let response = HttpFixedSource::make_fixed_response(200, Some(queue(b"ok"))).unwrap();
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

struct Outcome {
    result: Result<(), HttpErrorCode>,
    body: Vec<u8>,
    reads_issued: usize,
    written: Vec<u8>,
    close: CloseKind,
}

fn queue(bytes: &'static [u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap()
}

fn bytes_of(data: &BufQueue) -> Vec<u8> {
    data.segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

/// Serves one H1 request read through `ExternalTransport` under a downstream
/// `UniplexSession`.
fn serve(script: Vec<(&'static [u8], bool)>) -> Outcome {
    crate::uniplex_tests::run_on_worker(move |context| {
        Box::pin(async move {
            let transport = Rc::new(ExternalTransport {
                reads: RefCell::new(
                    script
                        .into_iter()
                        .map(|(data, eof)| TransportRead::new(queue(data), eof))
                        .collect(),
                ),
                reads_issued: Cell::new(0),
                written: RefCell::new(Vec::new()),
                close: Cell::new(CloseKind::Open),
            });
            let body = Rc::new(RefCell::new(Vec::new()));
            let handler: HttpHandlerRef = Rc::new(BodyHandler {
                body: Rc::clone(&body),
            });
            let erased: Rc<dyn UniplexTransport> = transport.clone();
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            Outcome {
                result: result.map(drop).map_err(|error| error.code()),
                body: body.take(),
                reads_issued: transport.reads_issued.get(),
                written: transport.written.take(),
                close: transport.close.get(),
            }
        })
    })
}

fn assert_served_once(outcome: &Outcome, reads_issued: usize) {
    assert_eq!(outcome.result, Ok(()));
    assert_eq!(outcome.body, b"abc");
    assert_eq!(outcome.reads_issued, reads_issued);
    assert!(
        outcome.written.starts_with(b"HTTP/1.1 200 ") && outcome.written.ends_with(b"\r\n\r\nok"),
        "{:?}",
        String::from_utf8_lossy(&outcome.written)
    );
    assert_eq!(outcome.close, CloseKind::ShutdownWrite);
}

#[test]
fn transport_read_new_round_trips_data_and_eof() {
    let first = Bytes::from(b"tunnel ".to_vec());
    let second = Bytes::from(b"bytes".to_vec());
    let mut data = BufQueue::new(16);
    data.push_bytes(first.clone()).unwrap();
    data.push_bytes(second.clone()).unwrap();

    let read = TransportRead::new(data, true);
    assert_eq!(read.bytes_read(), 12);
    assert!(read.is_eof());
    assert_eq!(read.data().limit(), 16);
    let segments = read.into_data().into_bytes_segments();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].as_ptr(), first.as_ptr());
    assert_eq!(segments[0], b"tunnel ".as_slice());
    assert_eq!(segments[1].as_ptr(), second.as_ptr());
    assert_eq!(segments[1], b"bytes".as_slice());

    let read = TransportRead::new(queue(b"abc"), false);
    assert_eq!(read.bytes_read(), 3);
    assert!(!read.is_eof());

    let read = TransportRead::new(BufQueue::new(0), true);
    assert_eq!(read.bytes_read(), 0);
    assert!(read.is_eof());

    let read = TransportRead::new(BufQueue::new(0), false);
    assert_eq!(read.bytes_read(), 0);
    assert!(!read.is_eof());
}

#[test]
fn external_transport_delivers_data_and_eof_under_uniplex() {
    assert_served_once(&serve(vec![(REQUEST, true)]), 1);
}

#[test]
fn external_transport_empty_read_is_not_eof_under_uniplex() {
    assert_served_once(&serve(vec![(b"", false), (REQUEST, false), (b"", true)]), 3);
}

/// A native TCP byte stream offers its poll-based reader through `UniplexTransport`, through
/// which the owned drivers read.
#[test]
fn tcp_byte_stream_offers_its_reader_through_uniplex() {
    use tokio::io::AsyncWriteExt;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(tokio::task::LocalSet::new().run_until(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (worker, driver) = roxygen_runtime::driver::WorkerDriver::new(8).unwrap();
            let call = worker
                .call_local(move |context| async move {
                    let accept = async { listener.accept().await.unwrap().0 };
                    let connect = roxygen_transport::tcp::TcpTransport::connect(
                        &context,
                        SocketAddress::new("127.0.0.1", port),
                        roxygen_transport::byte_stream::ByteStreamLimits::new(1024, 1024, 8)
                            .unwrap(),
                        Duration::from_secs(5),
                        CancellationToken::never(),
                    );
                    let (mut peer, transport) = tokio::join!(accept, connect);
                    let transport: Rc<dyn UniplexTransport> = Rc::new(transport.unwrap());
                    assert!(
                        transport
                            .try_reader(
                                &context,
                                Duration::from_secs(5),
                                CancellationToken::never()
                            )
                            .unwrap()
                            .is_none(),
                        "a read timeout keeps the read API"
                    );
                    let mut reader = transport
                        .try_reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .expect("an untimed TCP transport offers its reader");
                    peer.write_all(b"ok").await.unwrap();
                    reader.start_read(1024, 1024).unwrap();
                    let read = std::future::poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap();
                    assert!(!read.is_eof());
                    assert_eq!(bytes_of(&read.into_data()), b"ok");
                    drop(peer);
                    reader.start_read(1024, 1024).unwrap();
                    let eof = std::future::poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap();
                    assert!(eof.is_eof(), "the peer's close ends the next read");
                    transport.close(&context).unwrap();
                })
                .unwrap();
            let driver_task = tokio::task::spawn_local(driver);
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("the reader test is bounded")
                .unwrap();
            worker.begin_shutdown();
            driver_task.await.unwrap();
        }));
}

/// A native TCP byte stream offers its write queue through `UniplexTransport`, and the queue
/// answers `is_saturated`, so the owned drivers' `DIV-028` rule applies on TCP.
#[test]
fn tcp_byte_stream_offers_its_write_queue_through_uniplex() {
    use tokio::io::AsyncReadExt;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(tokio::task::LocalSet::new().run_until(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (worker, driver) = roxygen_runtime::driver::WorkerDriver::new(8).unwrap();
            let call = worker
                .call_local(move |context| async move {
                    let accept = async { listener.accept().await.unwrap().0 };
                    let connect = roxygen_transport::tcp::TcpTransport::connect(
                        &context,
                        SocketAddress::new("127.0.0.1", port),
                        roxygen_transport::byte_stream::ByteStreamLimits::new(1024, 1024, 8)
                            .unwrap(),
                        Duration::from_secs(5),
                        CancellationToken::never(),
                    );
                    let (mut peer, transport) = tokio::join!(accept, connect);
                    let transport: Rc<dyn UniplexTransport> = Rc::new(transport.unwrap());
                    assert!(
                        transport
                            .try_write_queue(
                                &context,
                                1024,
                                Duration::from_secs(5),
                                CancellationToken::never(),
                            )
                            .unwrap()
                            .is_none(),
                        "a write timeout keeps the single-write API"
                    );
                    let mut writer = transport
                        .try_write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .expect("an untimed TCP transport offers its native write queue");
                    assert!(
                        !writer.is_saturated(),
                        "an idle native queue answers unsaturated"
                    );
                    assert!(writer.can_write(2));
                    writer.start_write(queue(b"ok")).unwrap();
                    assert!(writer.is_saturated(), "its write is outstanding");
                    let completion =
                        std::future::poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(completion.requested_bytes, 2);
                    assert_eq!(completion.result.unwrap().bytes_written, 2);
                    assert!(writer.is_idle());
                    assert!(!writer.is_saturated());
                    let mut received = [0; 2];
                    peer.read_exact(&mut received).await.unwrap();
                    assert_eq!(&received, b"ok");
                    transport.close(&context).unwrap();
                })
                .unwrap();
            let driver_task = tokio::task::spawn_local(driver);
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("the queue test is bounded")
                .unwrap();
            worker.begin_shutdown();
            driver_task.await.unwrap();
        }));
}
