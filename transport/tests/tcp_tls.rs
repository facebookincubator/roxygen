mod tcp_tls {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    #[cfg(unix)]
    use std::error::Error;
    use std::future::Future;
    use std::future::poll_fn;
    use std::io;
    use std::io::Read;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    #[cfg(unix)]
    use std::panic::AssertUnwindSafe;
    #[cfg(target_os = "linux")]
    use std::panic::catch_unwind;
    #[cfg(unix)]
    use std::panic::panic_any;
    #[cfg(unix)]
    use std::panic::resume_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    #[cfg(unix)]
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;

    #[cfg(unix)]
    use futures::FutureExt;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::HttpByteEvent;
    use roxygen_core::byte_event::HttpByteEventCallback;
    use roxygen_core::byte_event::HttpByteEventRegistration;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::ids::StreamId;
    use roxygen_core::message::SocketAddress;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    #[cfg(unix)]
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_runtime::worker::WorkerHandle;
    use roxygen_runtime::worker::WorkerState;
    use rustls::ClientConfig;
    use rustls::RootCertStore;
    use rustls::ServerConfig;
    use rustls::crypto::cipher::OutboundChunks;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::ServerName;
    use tokio::io::AsyncBufRead;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWrite;
    use tokio::io::AsyncWriteExt;
    #[cfg(target_os = "linux")]
    use tokio::io::Interest;
    use tokio::io::ReadBuf;
    use tokio::net::TcpListener;
    #[cfg(target_os = "linux")]
    use tokio::net::TcpSocket;
    use tokio::net::TcpStream;
    use tokio::runtime::Builder;
    #[cfg(unix)]
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;
    use tokio::task::coop;
    use tokio::time;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::TlsConnector;

    use crate::byte_event::ByteEventWriteFlags;
    use crate::byte_stream::ByteEventCapability;
    use crate::byte_stream::ByteEventWriteState;
    use crate::byte_stream::ByteStreamLimits;
    use crate::byte_stream::ByteStreamTransport;
    use crate::byte_stream::LocalTransportFuture;
    use crate::byte_stream::OwnedByteStream;
    use crate::byte_stream::PeerIdentity;
    use crate::byte_stream::TransportEndpoint;
    use crate::byte_stream::TransportRead;
    #[cfg(target_os = "linux")]
    use crate::byte_stream::TransportRetirement;
    #[cfg(target_os = "linux")]
    use crate::byte_stream::TransportRetirementError;
    use crate::byte_stream::close_path_errors_for_test;
    #[cfg(target_os = "linux")]
    use crate::established::AdoptedIoCompletion;
    #[cfg(target_os = "linux")]
    use crate::established::AdoptedIoState;
    use crate::established::EstablishedIo;
    #[cfg(target_os = "linux")]
    use crate::established::IoDropPublisher;
    use crate::established::adopt_local;
    use crate::tcp::AcceptedTcpStream;
    use crate::tcp::AwaitError;
    use crate::tcp::ConnectDeadline;
    use crate::tcp::OwnedRead;
    use crate::tcp::TcpConnectOptions;
    #[cfg(unix)]
    use crate::tcp::TcpSocketOption;
    #[cfg(unix)]
    use crate::tcp::TcpSocketOptionPhase;
    #[cfg(unix)]
    use crate::tcp::TcpSocketOptionValue;
    use crate::tcp::TcpTransport;
    use crate::tcp::TokioByteStream;
    use crate::tcp::await_connect_with_deadline;
    #[cfg(unix)]
    use crate::tcp::connect_socket_with_options;
    use crate::tcp::last_tcp_first_reservation_for_test;
    use crate::tcp::owned_from_stream;
    use crate::tcp::setup_future;
    use crate::tcp::socket_address;
    use crate::tcp::tcp_first_reservations_for_test;
    use crate::tcp_zero_copy::SendOutcome;
    use crate::tcp_zero_copy::TcpZeroCopy;
    use crate::tls::CiphertextBackgroundForTest;
    use crate::tls::OwnedCiphertextStream;
    use crate::tls::OwnedTlsStream;
    use crate::tls::TlsClientOptions;
    use crate::tls::TlsPeerIdentityVerifier;
    use crate::tls::TlsServerOptions;
    use crate::tls::TlsStreamAccess;
    use crate::tls::TlsTransport;
    use crate::tls::await_handshake_with_deadline;
    use crate::tls::tls_error;

    std::thread_local! {
        static ESCAPED_TCP: RefCell<Option<(
            OwnedByteStream,
            LocalTransportFuture<Result<TransportRead, HttpError>>,
        )>> = const { RefCell::new(None) };
        static ESCAPED_SETUP: RefCell<
            Option<LocalTransportFuture<Result<(), HttpError>>>,
        > = const { RefCell::new(None) };
        static ESCAPED_HANDSHAKE: RefCell<
            Option<LocalTransportFuture<Result<TlsTransport, HttpError>>>,
        > = const { RefCell::new(None) };
        static ESCAPED_DRAIN: RefCell<Option<OwnedByteStream>> = const { RefCell::new(None) };
    }

    #[derive(Default)]
    struct WakeSignal {
        woken: AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl WakeSignal {
        fn take(&self) -> bool {
            self.woken.swap(false, Ordering::AcqRel)
        }

        async fn wait(&self) {
            while !self.take() {
                self.notify.notified().await;
            }
        }
    }

    #[test]
    fn mapped_ipv6_socket_addresses_are_exposed_as_ipv4() {
        let mapped = SocketAddr::new(
            IpAddr::V6(Ipv4Addr::new(192, 0, 2, 17).to_ipv6_mapped()),
            443,
        );
        assert_eq!(
            socket_address(mapped),
            SocketAddress::new("192.0.2.17", 443)
        );
    }

    impl Wake for WakeSignal {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Release);
            self.notify.notify_one();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.woken.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    struct SetupDropProbe {
        drops: Arc<AtomicUsize>,
        states: Arc<std::sync::Mutex<Vec<WorkerState>>>,
        threads: Arc<std::sync::Mutex<Vec<ThreadId>>>,
        worker: WorkerHandle,
    }

    impl Drop for SetupDropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
            self.states.lock().unwrap().push(self.worker.state());
            self.threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum DrainAction {
        Write(ThreadId),
        ShutdownReady(ThreadId),
        ShutdownPending(ThreadId),
        Drop(WorkerState, ThreadId),
    }

    #[derive(Default)]
    struct DrainState {
        write_ready: bool,
        write_error: bool,
        shutdown_permits: usize,
        write_waker: Option<Waker>,
        shutdown_waker: Option<Waker>,
        observer_waker: Option<Waker>,
        actions: Vec<DrainAction>,
    }

    #[derive(Clone, Default)]
    struct DrainControl(Arc<std::sync::Mutex<DrainState>>);

    impl DrainControl {
        fn allow_write(&self) {
            let waker = {
                let mut state = self.0.lock().unwrap();
                state.write_ready = true;
                state.write_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn fail_write(&self) {
            let waker = {
                let mut state = self.0.lock().unwrap();
                state.write_ready = true;
                state.write_error = true;
                state.write_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn permit_shutdowns(&self, count: usize) {
            let waker = {
                let mut state = self.0.lock().unwrap();
                state.shutdown_permits += count;
                state.shutdown_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn actions(&self) -> Vec<DrainAction> {
            self.0.lock().unwrap().actions.clone()
        }

        async fn wait_for_action(&self, predicate: impl Fn(&DrainAction) -> bool) {
            std::future::poll_fn(|context| {
                let mut state = self.0.lock().unwrap();
                if state.actions.iter().any(&predicate) {
                    Poll::Ready(())
                } else {
                    state.observer_waker = Some(context.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }
    }

    struct DrainStream {
        control: DrainControl,
        worker: WorkerHandle,
    }

    impl AsyncRead for DrainStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for DrainStream {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut state = self.control.0.lock().unwrap();
            if !state.write_ready {
                state.write_waker = Some(context.waker().clone());
                return Poll::Pending;
            }
            state
                .actions
                .push(DrainAction::Write(std::thread::current().id()));
            if state.write_error {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
            } else {
                Poll::Ready(Ok(buffer.len()))
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut state = self.control.0.lock().unwrap();
            let action = if state.shutdown_permits == 0 {
                state.shutdown_waker = Some(context.waker().clone());
                DrainAction::ShutdownPending(std::thread::current().id())
            } else {
                state.shutdown_permits -= 1;
                DrainAction::ShutdownReady(std::thread::current().id())
            };
            state.actions.push(action.clone());
            if let Some(waker) = state.observer_waker.take() {
                waker.wake();
            }
            match action {
                DrainAction::ShutdownPending(_) => Poll::Pending,
                DrainAction::ShutdownReady(_) => Poll::Ready(Ok(())),
                _ => unreachable!(),
            }
        }
    }

    impl TokioByteStream for DrainStream {
        fn prepare_reset(&self) {}
    }

    /// A stream that offers the native write queue, whose first write panics and whose later
    /// writes take every byte into `written`.
    struct FirstWritePanicStream {
        panicked: bool,
        written: Rc<RefCell<Vec<u8>>>,
    }

    impl AsyncRead for FirstWritePanicStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for FirstWritePanicStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            if !std::mem::replace(&mut self.panicked, true) {
                panic!("the first write attempt panics");
            }
            self.written.borrow_mut().extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl TokioByteStream for FirstWritePanicStream {
        const WRITE_QUEUE: bool = true;

        fn prepare_reset(&self) {}
    }

    struct BackingProbeStream {
        read_payload: Box<[u8]>,
        read_offset: usize,
        read_allocations: Rc<RefCell<Vec<usize>>>,
        write_pointers: Rc<RefCell<Vec<Vec<usize>>>>,
    }

    impl AsyncRead for BackingProbeStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let start = self.read_offset;
            if start == self.read_payload.len() {
                return Poll::Ready(Ok(()));
            }
            self.read_allocations
                .borrow_mut()
                .push(buffer.initialize_unfilled().as_ptr() as usize);
            let end = self.read_payload.len().min(start + buffer.remaining());
            buffer.put_slice(&self.read_payload[start..end]);
            self.read_offset = end;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for BackingProbeStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.write_pointers
                .borrow_mut()
                .push(vec![buffer.as_ptr() as usize]);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffers: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.write_pointers.borrow_mut().push(
                buffers
                    .iter()
                    .map(|buffer| buffer.as_ptr() as usize)
                    .collect(),
            );
            Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum()))
        }
    }

    impl TokioByteStream for BackingProbeStream {
        fn prepare_reset(&self) {}
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ReadPoll {
        offered: usize,
        /// Bytes read, or `None` when the poll did not complete with data or EOF.
        filled: Option<usize>,
    }

    /// Delegates to a real TCP stream and records every read poll.
    struct ReadPollProbe {
        stream: TcpStream,
        polls: Rc<RefCell<Vec<ReadPoll>>>,
    }

    impl AsyncRead for ReadPollProbe {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let offered = buffer.remaining();
            let before = buffer.filled().len();
            let result = Pin::new(&mut self.stream).poll_read(context, buffer);
            let filled =
                matches!(result, Poll::Ready(Ok(()))).then(|| buffer.filled().len() - before);
            self.polls.borrow_mut().push(ReadPoll { offered, filled });
            result
        }
    }

    impl AsyncWrite for ReadPollProbe {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.stream).poll_write(context, buffer)
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(context)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(context)
        }
    }

    impl TokioByteStream for ReadPollProbe {
        fn prepare_reset(&self) {}
    }

    /// Checks that the pending polls before a read's first bytes each offered
    /// `allocation_size`, and returns the later polls as (offered, filled).
    fn data_polls(polls: Vec<ReadPoll>, allocation_size: usize) -> Vec<(usize, usize)> {
        let first_data = polls
            .iter()
            .position(|poll| poll.filled.is_some())
            .unwrap_or(polls.len());
        assert!(
            polls[..first_data]
                .iter()
                .all(|poll| poll.offered == allocation_size),
            "{polls:?}",
        );
        polls[first_data..]
            .iter()
            .map(|poll| {
                let filled = poll.filled.unwrap_or_else(|| panic!("{polls:?}"));
                (poll.offered, filled)
            })
            .collect()
    }

    fn pattern(length: usize, seed: u8) -> Vec<u8> {
        (0..length)
            .map(|index| (index % 251) as u8 ^ seed)
            .collect()
    }

    #[derive(Clone, Copy)]
    enum CiphertextSend {
        Pending,
        Write(usize),
        Error(io::ErrorKind),
    }

    #[derive(Default)]
    struct CiphertextProbeState {
        sends: VecDeque<CiphertextSend>,
        borrowed_sends: VecDeque<CiphertextSend>,
        borrowed_pointers: Vec<usize>,
        begin_lengths: Vec<usize>,
        zero_copy_flags: Vec<bool>,
        attempted_pointers: Vec<usize>,
        written: Vec<u8>,
        operations: Vec<&'static str>,
        zero_copy_threshold: usize,
        reset: bool,
        abandoned: bool,
        send_waker: Option<Waker>,
        observer_waker: Option<Waker>,
    }

    #[derive(Clone, Default)]
    struct CiphertextProbeControl(Rc<RefCell<CiphertextProbeState>>);

    impl CiphertextProbeControl {
        fn stream(&self, sends: impl IntoIterator<Item = CiphertextSend>) -> CiphertextProbeStream {
            self.0.borrow_mut().sends.extend(sends);
            CiphertextProbeStream {
                control: self.clone(),
            }
        }

        fn allow_send(&self, written: usize) {
            let waker = {
                let mut state = self.0.borrow_mut();
                state.sends.push_back(CiphertextSend::Write(written));
                state.send_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        async fn wait_written(&self, length: usize) {
            std::future::poll_fn(|context| {
                let mut state = self.0.borrow_mut();
                if state.written.len() >= length {
                    Poll::Ready(())
                } else {
                    state.observer_waker = Some(context.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }
    }

    struct CiphertextProbeStream {
        control: CiphertextProbeControl,
    }

    impl AsyncRead for CiphertextProbeStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for CiphertextProbeStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned-write hook is required",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.control.0.borrow_mut().operations.push("flush");
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.control.0.borrow_mut().operations.push("shutdown");
            Poll::Ready(Ok(()))
        }
    }

    impl TokioByteStream for CiphertextProbeStream {
        fn prepare_reset(&self) {
            self.control.0.borrow_mut().reset = true;
        }

        fn begin_owned_write(&mut self, length: usize) -> bool {
            let mut state = self.control.0.borrow_mut();
            state.begin_lengths.push(length);
            length >= state.zero_copy_threshold
        }

        fn try_write_borrowed(
            &mut self,
            _context: &mut Context<'_>,
            buffers: &[io::IoSlice<'_>],
        ) -> io::Result<usize> {
            let mut state = self.control.0.borrow_mut();
            let Some(send) = state.borrowed_sends.pop_front() else {
                return Err(io::ErrorKind::WouldBlock.into());
            };
            state.borrowed_pointers.push(buffers[0].as_ptr() as usize);
            match send {
                CiphertextSend::Pending => Err(io::ErrorKind::WouldBlock.into()),
                CiphertextSend::Write(written) => {
                    state.written.extend(
                        buffers
                            .iter()
                            .flat_map(|buffer| buffer.iter().copied())
                            .take(written),
                    );
                    state.operations.push("borrowed write");
                    Ok(written)
                }
                CiphertextSend::Error(kind) => Err(kind.into()),
            }
        }

        fn poll_write_owned(
            &mut self,
            context: &mut Context<'_>,
            data: &BufQueue,
            zero_copy: bool,
        ) -> Poll<io::Result<usize>> {
            let mut state = self.control.0.borrow_mut();
            state.zero_copy_flags.push(zero_copy);
            state.attempted_pointers.push(
                data.front()
                    .expect("ciphertext write has a first segment")
                    .as_ptr() as usize,
            );
            match state.sends.pop_front().unwrap_or(CiphertextSend::Pending) {
                CiphertextSend::Pending => {
                    state.send_waker = Some(context.waker().clone());
                    Poll::Pending
                }
                CiphertextSend::Write(written) => {
                    let available = data.chain_length();
                    assert!(written <= available, "scripted write must fit ciphertext");
                    state.written.extend(
                        data.segments()
                            .flat_map(|segment| segment.iter().copied())
                            .take(written),
                    );
                    state.operations.push("write");
                    if let Some(waker) = state.observer_waker.take() {
                        waker.wake();
                    }
                    Poll::Ready(Ok(written))
                }
                CiphertextSend::Error(kind) => Poll::Ready(Err(kind.into())),
            }
        }

        fn abandon_zero_copy(&mut self) {
            self.control.0.borrow_mut().abandoned = true;
        }

        #[cfg(test)]
        fn zero_copy_owner_for_test(&self) -> Option<usize> {
            Some(7)
        }
    }

    #[derive(Clone, Copy)]
    enum TlsFlushAction {
        Pending,
        Ready,
        Error(io::ErrorKind),
    }

    struct ScriptedTlsStream<S = CiphertextProbeStream>
    where
        S: TokioByteStream,
    {
        transport: OwnedCiphertextStream<S>,
        ciphertext: &'static [u8],
        accepted_plaintext: usize,
        flushes: VecDeque<TlsFlushAction>,
        plaintext_writes: Rc<Cell<usize>>,
        wants_write: bool,
        wants_write_after_accept: bool,
        block_after_first: bool,
    }

    impl<S> AsyncRead for ScriptedTlsStream<S>
    where
        S: TokioByteStream,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.transport).poll_read(context, buffer)
        }
    }

    impl<S> AsyncBufRead for ScriptedTlsStream<S>
    where
        S: TokioByteStream,
    {
        fn poll_fill_buf(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<&[u8]>> {
            let mut buffer = ReadBuf::new(&mut []);
            Pin::new(&mut self.transport)
                .poll_read(context, &mut buffer)
                .map_ok(|()| &[][..])
        }

        fn consume(self: Pin<&mut Self>, amount: usize) {
            assert_eq!(amount, 0);
        }
    }

    impl<S> AsyncWrite for ScriptedTlsStream<S>
    where
        S: TokioByteStream,
    {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            plaintext: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.block_after_first && self.plaintext_writes.get() != 0 {
                return Poll::Pending;
            }
            let ciphertext = self.ciphertext;
            match Pin::new(&mut self.transport).poll_write(context, ciphertext) {
                Poll::Ready(Ok(_)) => {
                    self.plaintext_writes.set(self.plaintext_writes.get() + 1);
                    self.wants_write = self.wants_write_after_accept;
                    Poll::Ready(Ok(self.accepted_plaintext.min(plaintext.len())))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.flushes.pop_front().unwrap_or(TlsFlushAction::Ready) {
                TlsFlushAction::Pending => Poll::Pending,
                TlsFlushAction::Ready => {
                    self.wants_write = false;
                    Pin::new(&mut self.transport).poll_flush(context)
                }
                TlsFlushAction::Error(kind) => {
                    self.wants_write = false;
                    Poll::Ready(Err(kind.into()))
                }
            }
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.transport).poll_shutdown(context)
        }
    }

    impl<S> TlsStreamAccess for ScriptedTlsStream<S>
    where
        S: TokioByteStream,
    {
        type Tcp = S;

        fn transport(&self) -> &OwnedCiphertextStream<Self::Tcp> {
            &self.transport
        }

        fn transport_mut(&mut self) -> &mut OwnedCiphertextStream<Self::Tcp> {
            &mut self.transport
        }

        fn wants_write(&self) -> bool {
            self.wants_write
        }
    }

    struct ZeroCopyLifecycleStream {
        controller: Rc<RefCell<TcpZeroCopy>>,
        sends: Rc<RefCell<VecDeque<io::Result<usize>>>>,
        abandoned: Arc<AtomicBool>,
        shutdown_complete: Arc<AtomicBool>,
    }

    impl AsyncRead for ZeroCopyLifecycleStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for ZeroCopyLifecycleStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.controller.borrow_mut().poll_drain(context) {
                Poll::Ready(()) => {
                    self.shutdown_complete.store(true, Ordering::Release);
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl TokioByteStream for ZeroCopyLifecycleStream {
        fn prepare_reset(&self) {}

        fn enable_zero_copy(
            &self,
            _context: &roxygen_runtime::worker::WorkerContext,
            threshold: usize,
        ) -> io::Result<bool> {
            Ok(self.controller.borrow_mut().enable_for_test(threshold))
        }

        fn set_zero_copy_reenable_threshold(&self, threshold: usize) {
            self.controller
                .borrow_mut()
                .set_reenable_threshold(threshold);
        }

        fn begin_owned_write(&mut self, length: usize) -> bool {
            self.controller.borrow_mut().begin_write(length)
        }

        fn poll_write_owned(
            &mut self,
            _context: &mut Context<'_>,
            data: &BufQueue,
            zero_copy: bool,
        ) -> Poll<io::Result<usize>> {
            let result = self
                .sends
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| Err(io::ErrorKind::WouldBlock.into()));
            let (outcome, _) =
                self.controller
                    .borrow_mut()
                    .write_after_ready_for_test(data, zero_copy, [result]);
            match outcome {
                SendOutcome::Written(written) => Poll::Ready(Ok(written)),
                SendOutcome::Wait => Poll::Pending,
                SendOutcome::Error(error) => Poll::Ready(Err(error)),
                SendOutcome::RetryOrdinary => {
                    unreachable!("the production decision path consumes ordinary retries")
                }
            }
        }

        fn abandon_zero_copy(&mut self) {
            self.controller.borrow_mut().force_teardown();
            self.abandoned.store(true, Ordering::Release);
        }

        #[cfg(test)]
        fn zero_copy_owner_for_test(&self) -> Option<usize> {
            Some(self.controller.borrow().completion_owner_for_test())
        }

        #[cfg(test)]
        fn zero_copy_pending_for_test(&self) -> usize {
            self.controller.borrow().pending_count_for_test()
        }
    }

    impl Drop for DrainStream {
        fn drop(&mut self) {
            let mut state = self.control.0.lock().unwrap();
            state.actions.push(DrainAction::Drop(
                self.worker.state(),
                std::thread::current().id(),
            ));
            if let Some(waker) = state.observer_waker.take() {
                waker.wake();
            }
        }
    }

    #[derive(Clone, Default)]
    struct CapableEventControl {
        flags: Rc<RefCell<Vec<ByteEventWriteFlags>>>,
        pending: Rc<RefCell<Vec<(u64, ByteEventWriteState)>>>,
    }

    impl CapableEventControl {
        fn notify(&self, event_type: ByteEventType, offset: u64) {
            let ready = {
                let mut pending = self.pending.borrow_mut();
                let mut ready = Vec::new();
                let mut retained = Vec::new();
                for (boundary, events) in std::mem::take(&mut *pending) {
                    if boundary <= offset {
                        ready.push(events.clone());
                    }
                    if boundary > offset || !events.is_empty() {
                        retained.push((boundary, events));
                    }
                }
                *pending = retained;
                ready
            };
            for events in ready {
                events.fire(event_type, offset);
            }
            self.pending
                .borrow_mut()
                .retain(|(_, events)| !events.is_empty());
        }
    }

    struct CapableDrainStream {
        inner: DrainStream,
        events: CapableEventControl,
    }

    impl AsyncRead for CapableDrainStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(context, buffer)
        }
    }

    impl AsyncWrite for CapableDrainStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(context, buffer)
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(context)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(context)
        }
    }

    impl TokioByteStream for CapableDrainStream {
        fn prepare_reset(&self) {}

        fn byte_event_capability(&self) -> ByteEventCapability {
            ByteEventCapability::TransportEvents
        }

        fn register_byte_events(
            &mut self,
            flags: ByteEventWriteFlags,
            transport_offset: u64,
            byte_events: Option<ByteEventWriteState>,
        ) -> io::Result<()> {
            self.events.flags.borrow_mut().push(flags);
            if let Some(byte_events) = byte_events {
                self.events
                    .pending
                    .borrow_mut()
                    .push((transport_offset, byte_events));
            }
            Ok(())
        }

        fn take_byte_events(&mut self) -> Vec<ByteEventWriteState> {
            std::mem::take(&mut *self.events.pending.borrow_mut())
                .into_iter()
                .map(|(_, events)| events)
                .collect()
        }
    }

    #[derive(Default)]
    struct TransportEventLog {
        fired: RefCell<Vec<HttpByteEvent>>,
        cancelled: RefCell<Vec<(HttpByteEvent, HttpErrorCode)>>,
    }

    impl HttpByteEventCallback for TransportEventLog {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.fired.borrow_mut().push(event);
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled.borrow_mut().push((event, error.code()));
        }
    }

    fn limits() -> ByteStreamLimits {
        ByteStreamLimits::new(64, 1024, 8).unwrap()
    }

    fn address(address: std::net::SocketAddr) -> SocketAddress {
        SocketAddress::new(address.ip().to_string(), address.port())
    }

    fn queue(segments: &[&[u8]]) -> BufQueue {
        let length = segments.iter().map(|segment| segment.len()).sum();
        let mut queue = BufQueue::new(length);
        for segment in segments {
            queue.push_copy(segment).unwrap();
        }
        queue
    }

    fn zero_copy_lifecycle_transport(
        context: &roxygen_runtime::worker::WorkerContext,
        sends: impl IntoIterator<Item = io::Result<usize>>,
        abandoned: Arc<AtomicBool>,
        shutdown_complete: Arc<AtomicBool>,
    ) -> (OwnedByteStream, Rc<RefCell<TcpZeroCopy>>) {
        let controller = Rc::new(RefCell::new(TcpZeroCopy::new()));
        let stream = ZeroCopyLifecycleStream {
            controller: Rc::clone(&controller),
            sends: Rc::new(RefCell::new(sends.into_iter().collect())),
            abandoned,
            shutdown_complete,
        };
        let transport = owned_from_stream(
            context,
            stream,
            SocketAddress::new("127.0.0.1", 1000),
            SocketAddress::new("127.0.0.1", 2000),
            None,
            limits(),
        )
        .expect("zero-copy lifecycle stream is adopted");
        (transport, controller)
    }

    fn read_bytes(read: TransportRead) -> Vec<u8> {
        read.into_data()
            .into_bytes_segments()
            .into_iter()
            .flat_map(|segment| segment.to_vec())
            .collect()
    }

    fn read_bytes_ref(read: &TransportRead) -> Vec<u8> {
        read.data()
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect()
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[cfg(unix)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum NoDelayEndpoint {
        Hostname,
        Numeric,
        BoundNumeric,
    }

    #[cfg(unix)]
    impl NoDelayEndpoint {
        /// `localhost` can resolve to `::1` first, so the hostname listener
        /// binds the same name; otherwise another socket on `[::1]:port` would
        /// take the connection.
        fn listen_address(self) -> &'static str {
            if self == Self::Hostname {
                "localhost:0"
            } else {
                "127.0.0.1:0"
            }
        }

        fn connection(
            self,
            destination: SocketAddr,
            socket_options: Vec<TcpSocketOption>,
        ) -> (SocketAddress, TcpConnectOptions) {
            let destination = if self == Self::Hostname {
                SocketAddress::new("localhost", destination.port())
            } else {
                address(destination)
            };
            let options = TcpConnectOptions {
                bind_address: (self == Self::BoundNumeric)
                    .then(|| SocketAddress::new("127.0.0.1", 0)),
                socket_options,
                congestion_flavor: None,
            };
            (destination, options)
        }
    }

    #[cfg(unix)]
    #[derive(Debug)]
    struct NoDelayCase {
        endpoint: NoDelayEndpoint,
        options: Vec<TcpSocketOption>,
        expected: bool,
    }

    #[cfg(unix)]
    fn no_delay_cases() -> Vec<NoDelayCase> {
        let mut cases = Vec::new();
        for endpoint in [
            NoDelayEndpoint::Hostname,
            NoDelayEndpoint::Numeric,
            NoDelayEndpoint::BoundNumeric,
        ] {
            cases.push(NoDelayCase {
                endpoint,
                options: Vec::new(),
                expected: true,
            });
            for phase in [
                TcpSocketOptionPhase::PreBind,
                TcpSocketOptionPhase::PreConnect,
                TcpSocketOptionPhase::PostConnect,
            ] {
                if endpoint == NoDelayEndpoint::Hostname
                    && phase != TcpSocketOptionPhase::PostConnect
                {
                    continue;
                }
                for expected in [false, true] {
                    cases.push(NoDelayCase {
                        endpoint,
                        options: vec![TcpSocketOption::no_delay_at(expected, phase)],
                        expected,
                    });
                }
            }
        }
        for endpoint in [NoDelayEndpoint::Numeric, NoDelayEndpoint::BoundNumeric] {
            for expected in [false, true] {
                cases.push(NoDelayCase {
                    endpoint,
                    options: vec![
                        TcpSocketOption::no_delay_at(expected, TcpSocketOptionPhase::PreConnect),
                        TcpSocketOption::no_delay_at(!expected, TcpSocketOptionPhase::PreBind),
                    ],
                    expected,
                });
                cases.push(NoDelayCase {
                    endpoint,
                    options: vec![
                        TcpSocketOption::no_delay(expected),
                        TcpSocketOption::no_delay_at(!expected, TcpSocketOptionPhase::PreConnect),
                        TcpSocketOption::no_delay_at(!expected, TcpSocketOptionPhase::PreBind),
                    ],
                    expected,
                });
            }
        }
        for expected in [false, true] {
            cases.push(NoDelayCase {
                endpoint: NoDelayEndpoint::Hostname,
                options: vec![
                    TcpSocketOption::no_delay(!expected),
                    TcpSocketOption::no_delay(expected),
                ],
                expected,
            });
        }
        cases
    }

    #[cfg(unix)]
    async fn observe_no_delay(context: &WorkerContext, case: &NoDelayCase) -> bool {
        let listener = TcpListener::bind(case.endpoint.listen_address())
            .await
            .expect("loopback listener");
        let destination = listener.local_addr().expect("listener address");
        let (destination, options) = case.endpoint.connection(destination, case.options.clone());
        let setup_context = context.clone();
        let setup = setup_future(context, async move {
            let deadline = ConnectDeadline::new(Duration::from_secs(1));
            let connected = connect_socket_with_options(
                destination,
                &deadline,
                CancellationToken::never(),
                options,
            )
            .await?;
            let actual = connected
                .stream
                .nodelay()
                .expect("actual connected TCP_NODELAY getter");
            let transport = owned_from_stream(
                &setup_context,
                AcceptedTcpStream::new(connected.stream),
                connected.local_address,
                connected.peer_address,
                None,
                limits(),
            )?;
            Ok((transport, actual))
        })
        .expect("owner-local connect setup admission");
        let (transport, actual) = setup.await.expect("actual shared TCP connect succeeds");
        let (mut peer, remote) = listener
            .accept()
            .await
            .expect("actual connected peer accepted");
        assert!(context.is_owner_thread());
        assert_eq!(
            transport
                .local_address(context)
                .expect("local endpoint")
                .port(),
            remote.port()
        );
        transport
            .close(context)
            .expect("schedule actual graceful transport close");
        let mut byte = [0];
        assert_eq!(
            peer.read(&mut byte)
                .await
                .expect("peer observes actual native FIN"),
            0
        );
        drop(peer);
        drop(transport);
        actual
    }

    #[cfg(unix)]
    async fn reject_no_delay_option(
        context: &WorkerContext,
        endpoint: NoDelayEndpoint,
        phase: TcpSocketOptionPhase,
    ) {
        let listener = TcpListener::bind(endpoint.listen_address())
            .await
            .expect("option-error listener");
        let destination = listener
            .local_addr()
            .expect("option-error listener address");
        let (destination, options) = endpoint.connection(
            destination,
            vec![TcpSocketOption {
                level: libc::IPPROTO_TCP,
                name: libc::TCP_NODELAY,
                value: TcpSocketOptionValue::Bytes(Vec::new().into_boxed_slice()),
                phase,
            }],
        );
        let setup = setup_future(context, async move {
            let deadline = ConnectDeadline::new(Duration::from_secs(1));
            connect_socket_with_options(destination, &deadline, CancellationToken::never(), options)
                .await
        })
        .expect("owner-local invalid-option setup admission");
        let error = match setup.await {
            Ok(connected) => {
                drop(connected);
                panic!(
                    "explicit invalid TCP_NODELAY unexpectedly succeeded: {endpoint:?}/{phase:?}"
                );
            }
            Err(error) => error,
        };
        if phase == TcpSocketOptionPhase::PostConnect {
            let (mut peer, _) = listener
                .accept()
                .await
                .expect("post-connect failure reached peer");
            let mut byte = [0];
            assert_eq!(
                peer.read(&mut byte)
                    .await
                    .expect("failed explicit option closes socket"),
                0
            );
        } else {
            let mut accept = Box::pin(listener.accept());
            assert!(
                poll_once(accept.as_mut()).is_pending(),
                "invalid early option precedes connect"
            );
        }
        assert_eq!(
            error.code(),
            HttpErrorCode::CONNECT_ERROR,
            "{endpoint:?}/{phase:?}: {error}"
        );
        assert!(context.is_owner_thread());
    }

    #[cfg(unix)]
    async fn run_no_delay_worker(
        operation: impl FnOnce(WorkerContext) -> LocalTransportFuture<()> + Send + 'static,
    ) {
        let (worker, driver) = WorkerDriver::new(4).expect("TCP option worker");
        let call = worker
            .call_local(move |context| async move {
                AssertUnwindSafe(async move { operation(context).await })
                    .catch_unwind()
                    .await
            })
            .expect("TCP option worker command admitted");
        let close_worker = async {
            let result = call.await;
            worker.begin_shutdown();
            result
        };
        let (_, result) = tokio::join!(driver, close_worker);
        assert_eq!(
            worker.state(),
            WorkerState::Closed,
            "actual inline driver joined"
        );
        if let Err(payload) = result.expect("TCP option command outcome after worker cleanup") {
            resume_unwind(payload);
        }
    }

    #[cfg(target_os = "linux")]
    #[derive(Default)]
    struct RetirementProbeState {
        flush: bool,
        shutdown: bool,
        flush_polled: bool,
        shutdown_polled: bool,
        shutdown_complete: bool,
        shutdown_error: Option<io::Error>,
        waker: Option<Waker>,
        observer: Option<Waker>,
        body_drops: usize,
        field_drops: usize,
        panic_on_drop: bool,
        receipt: Option<TransportRetirement>,
        field_observer: Option<Arc<AtomicUsize>>,
    }

    #[cfg(target_os = "linux")]
    struct RetirementField {
        state: Rc<RefCell<RetirementProbeState>>,
        owner: ThreadId,
    }

    #[cfg(target_os = "linux")]
    impl Drop for RetirementField {
        fn drop(&mut self) {
            assert_eq!(self.owner, std::thread::current().id());
            let receipt = self.state.borrow().receipt.clone();
            if let Some(receipt) = receipt {
                let mut waiting = Box::pin(receipt.wait_retired());
                assert!(
                    poll_once(waiting.as_mut()).is_pending(),
                    "field destruction precedes the I/O boundary"
                );
            }
            self.state.borrow_mut().field_drops += 1;
            if let Some(observer) = &self.state.borrow().field_observer {
                observer.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[cfg(target_os = "linux")]
    struct RetirementProbe {
        field: RetirementField,
    }

    #[cfg(target_os = "linux")]
    impl Drop for RetirementProbe {
        fn drop(&mut self) {
            assert_eq!(self.field.owner, std::thread::current().id());
            let panic_on_drop = {
                let mut state = self.field.state.borrow_mut();
                state.body_drops += 1;
                state.panic_on_drop
            };
            if panic_on_drop {
                panic_any("original native value destruction panic");
            }
        }
    }

    #[cfg(target_os = "linux")]
    impl AsyncRead for RetirementProbe {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[cfg(target_os = "linux")]
    impl AsyncWrite for RetirementProbe {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            let (ready, observer) = {
                let mut state = self.field.state.borrow_mut();
                state.flush_polled = true;
                if !state.flush {
                    state.waker = Some(context.waker().clone());
                }
                (state.flush, state.observer.take())
            };
            if let Some(observer) = observer {
                observer.wake();
            }
            if ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            match self.as_mut().poll_flush(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let (ready, error, observer) = {
                let mut state = self.field.state.borrow_mut();
                state.shutdown_polled = true;
                state.shutdown_complete = state.shutdown;
                if !state.shutdown {
                    state.waker = Some(context.waker().clone());
                }
                let error = if state.shutdown {
                    state.shutdown_error.take()
                } else {
                    None
                };
                (state.shutdown, error, state.observer.take())
            };
            if let Some(observer) = observer {
                observer.wake();
            }
            if ready {
                Poll::Ready(error.map_or(Ok(()), Err))
            } else {
                Poll::Pending
            }
        }
    }

    #[cfg(target_os = "linux")]
    impl TokioByteStream for RetirementProbe {
        fn prepare_reset(&self) {}
    }

    #[cfg(target_os = "linux")]
    async fn retirement_probe_phase(
        state: &Rc<RefCell<RetirementProbeState>>,
        predicate: impl Fn(&RetirementProbeState) -> bool,
    ) {
        poll_fn(|context| {
            let mut state = state.borrow_mut();
            if predicate(&state) {
                Poll::Ready(())
            } else {
                state.observer = Some(context.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }

    #[cfg(target_os = "linux")]
    fn release_retirement_probe(state: &Rc<RefCell<RetirementProbeState>>, shutdown: bool) {
        let waker = {
            let mut state = state.borrow_mut();
            state.flush = true;
            state.shutdown = shutdown;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[cfg(target_os = "linux")]
    struct RetirementWake(AtomicUsize);

    #[cfg(target_os = "linux")]
    impl Wake for RetirementWake {
        fn wake(self: Arc<Self>) {
            let previous = self.0.fetch_add(1, Ordering::SeqCst);
            panic_any(if previous == 0 {
                "first retirement observer panic"
            } else {
                "secondary retirement observer panic"
            });
        }
    }

    #[cfg(target_os = "linux")]
    struct RetirementScopeDrop(Rc<Cell<usize>>);

    #[cfg(target_os = "linux")]
    impl Drop for RetirementScopeDrop {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
            panic_any("original scope capture destruction panic");
        }
    }

    #[cfg(target_os = "linux")]
    async fn retirement_uncertainty_control(context: &WorkerContext) {
        let drops = Rc::new(Cell::new(0));
        let owned = RetirementScopeDrop(Rc::clone(&drops));
        let registration = context
            .register_scope(async move {
                let _owned = owned;
                std::future::pending::<()>().await;
            })
            .unwrap();
        let scope = registration.observe();
        let shared = Arc::new(AdoptedIoState::default());
        let publisher = IoDropPublisher::new(Arc::clone(&shared));
        let receipt = TransportRetirement::new(AdoptedIoCompletion::new(shared, scope));
        let mut observing = Box::pin(receipt.wait_retired());
        assert!(poll_once(observing.as_mut()).is_pending());
        let panic = catch_unwind(AssertUnwindSafe(|| registration.disarm()))
            .expect_err("actual reserved scope capture destruction failed");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"original scope capture destruction panic")
        );
        assert_eq!(drops.get(), 1);
        assert!(
            poll_once(observing.as_mut()).is_pending(),
            "scope uncertainty still waits for the separate value publisher"
        );
        drop(publisher);
        assert_eq!(
            observing.await,
            Err(TransportRetirementError::BothUnconfirmed)
        );
        assert_eq!(
            receipt.wait_retired().await,
            Err(TransportRetirementError::BothUnconfirmed)
        );
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, thiserror::Error)]
    #[error("original ready shutdown error")]
    struct RetirementReadyError {
        errors: Arc<AtomicUsize>,
        payloads: Arc<AtomicUsize>,
    }

    #[cfg(target_os = "linux")]
    impl Drop for RetirementReadyError {
        fn drop(&mut self) {
            assert_eq!(self.errors.fetch_add(1, Ordering::SeqCst), 0);
            panic_any(RetirementReadyErrorPanic(Arc::clone(&self.payloads)));
        }
    }

    #[cfg(target_os = "linux")]
    struct RetirementReadyErrorPanic(Arc<AtomicUsize>);

    #[cfg(target_os = "linux")]
    impl Drop for RetirementReadyErrorPanic {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_add(1, Ordering::SeqCst), 0);
            panic_any("secondary shutdown-error panic payload destructor");
        }
    }

    #[cfg(target_os = "linux")]
    async fn retirement_ready_error_control() {
        let (worker, driver) = WorkerDriver::new(4).unwrap();
        let fields = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));
        let payloads = Arc::new(AtomicUsize::new(0));
        let notifications = Arc::new(RetirementWake(AtomicUsize::new(0)));
        let (publish, published) = oneshot::channel();
        let call = worker
            .call_local({
                let fields = Arc::clone(&fields);
                let errors = Arc::clone(&errors);
                let payloads = Arc::clone(&payloads);
                let notifications = Arc::clone(&notifications);
                move |context| async move {
                    let state = Rc::new(RefCell::new(RetirementProbeState {
                        field_observer: Some(fields),
                        ..RetirementProbeState::default()
                    }));
                    let transport = owned_from_stream(
                        &context,
                        RetirementProbe {
                            field: RetirementField {
                                state: Rc::clone(&state),
                                owner: std::thread::current().id(),
                            },
                        },
                        SocketAddress::default(),
                        SocketAddress::default(),
                        None,
                        limits(),
                    )
                    .unwrap();
                    let receipt = transport.retirement().unwrap();
                    state.borrow_mut().receipt = Some(receipt.clone());
                    state.borrow_mut().shutdown_error =
                        Some(io::Error::other(RetirementReadyError { errors, payloads }));
                    let waker = Waker::from(notifications);
                    let mut observer = Box::pin(receipt.wait_retired());
                    assert!(
                        observer
                            .as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    assert!(publish.send(receipt).is_ok());
                    transport.close(&context).unwrap();
                    retirement_probe_phase(&state, |state| state.flush_polled).await;
                    drop(transport);
                    release_retirement_probe(&state, true);
                    std::future::pending::<()>().await;
                    drop(observer);
                }
            })
            .unwrap();
        let independent = async {
            let receipt: TransportRetirement = published.await.unwrap();
            let outcome = receipt.wait_retired().await;
            (receipt, outcome)
        };
        let (driver_result, (receipt, outcome)) =
            tokio::join!(AssertUnwindSafe(driver).catch_unwind(), independent);
        let panic = driver_result.expect_err(
            "real close-scope notification failure remains observable after inline joining",
        );
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"first retirement observer panic")
        );
        assert_eq!(worker.state(), WorkerState::Closed);
        assert!(
            call.await.is_err(),
            "intentional worker failure cancels the retained command"
        );
        assert_eq!(
            outcome,
            Ok(()),
            "value/scope destruction is not I/O-success qualification"
        );
        assert_eq!(receipt.wait_retired().await, outcome);
        assert_eq!(fields.load(Ordering::SeqCst), 1);
        assert_eq!(errors.load(Ordering::SeqCst), 1);
        assert_eq!(payloads.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.0.load(Ordering::SeqCst), 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_retirement_waits_for_io_and_close_scope() {
        fn send_sync_clone<T: Send + Sync + Clone>() {}
        send_sync_clone::<TransportRetirement>();
        Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            time::timeout(Duration::from_secs(5), run_no_delay_worker(|context| Box::pin(async move {
                for mode in 0..6 {
                    let state = Rc::new(RefCell::new(RetirementProbeState::default()));
                    let transport = owned_from_stream(
                        &context,
                        RetirementProbe { field: RetirementField {
                            state: Rc::clone(&state), owner: std::thread::current().id(),
                        } },
                        SocketAddress::default(), SocketAddress::default(), None, limits(),
                    ).unwrap();
                    let receipt = transport.retirement().expect("ordinary native backend has a receipt");
                    state.borrow_mut().receipt = Some(receipt.clone());
                    let mut abandoned = Box::pin(receipt.wait_retired());
                    assert!(poll_once(abandoned.as_mut()).is_pending());
                    drop(abandoned);
                    let mut observing = Box::pin(receipt.wait_retired());
                    assert!(poll_once(observing.as_mut()).is_pending());
                    match mode {
                        0 => drop(transport),
                        1 | 4 => {
                            transport.shutdown_write(&context).unwrap();
                            state.borrow_mut().panic_on_drop = mode == 4;
                            let reset = catch_unwind(AssertUnwindSafe(|| transport.close_with_reset(&context)));
                            if mode == 4 {
                                let payload = reset.expect_err("original backend Drop panic reaches caller");
                                assert_eq!(payload.downcast_ref::<&str>(), Some(&"original native value destruction panic"));
                            } else {
                                reset.unwrap().unwrap();
                            }
                            assert_eq!(state.borrow().field_drops, 1);
                            assert!(poll_once(observing.as_mut()).is_pending(), "disposed I/O does not short-circuit an active close scope, including uncertainty");
                            drop(transport);
                        }
                        2 | 3 => {
                            if mode == 2 { transport.close(&context).unwrap(); }
                            else { transport.shutdown_write(&context).unwrap(); }
                            retirement_probe_phase(&state, |state| state.flush_polled).await;
                            assert!(poll_once(observing.as_mut()).is_pending());
                            release_retirement_probe(&state, false);
                            retirement_probe_phase(&state, |state| state.shutdown_polled).await;
                            assert!(poll_once(observing.as_mut()).is_pending());
                            assert_eq!(state.borrow().field_drops, 0);
                            if mode == 2 {
                                drop(transport);
                                assert_eq!(state.borrow().body_drops, 0, "root drop preserves the admitted drain");
                                release_retirement_probe(&state, true);
                            } else {
                                release_retirement_probe(&state, true);
                                retirement_probe_phase(&state, |state| state.shutdown_complete).await;
                                assert!(poll_once(observing.as_mut()).is_pending(), "half-close is not I/O value destruction");
                                transport.close(&context).unwrap();
                                drop(transport);
                            }
                        }
                        5 => {
                            let wake = Arc::new(RetirementWake(AtomicUsize::new(0)));
                            let waker = Waker::from(Arc::clone(&wake));
                            let mut panicking = Box::pin(receipt.wait_retired());
                            assert!(panicking.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
                            let panic = catch_unwind(AssertUnwindSafe(|| transport.close_with_reset(&context)))
                                .expect_err("first notification panic remains caller-visible");
                            assert_eq!(panic.downcast_ref::<&str>(), Some(&"first retirement observer panic"));
                            assert_eq!(wake.0.load(Ordering::SeqCst), 2, "both independent publishers attempted notification");
                            assert_eq!(state.borrow().field_drops, 1, "notification panic cannot skip complete I/O destruction");
                            drop(panicking);
                            drop(transport);
                        }
                        _ => unreachable!(),
                    }
                    let expected = if mode == 4 { Err(TransportRetirementError::IoDestructionUnconfirmed) } else { Ok(()) };
                    assert_eq!(observing.await, expected);
                    assert_eq!(receipt.wait_retired().await, expected);
                    assert_eq!(state.borrow().body_drops, 1);
                    assert_eq!(state.borrow().field_drops, 1);
                    assert!(context.is_owner_thread() && context.handle().is_accepting());
                }
                retirement_uncertainty_control(&context).await;
            }))).await.expect("bounded real native close scopes and owned values");
            time::timeout(Duration::from_secs(5), retirement_ready_error_control()).await
                .expect("bounded original notification/error/payload retirement with actual inline driver completion");
        });
    }

    #[cfg(unix)]
    #[test]
    fn tcp_connect_no_delay_default_and_overrides() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("TCP option reactor")
            .block_on(LocalSet::new().run_until(async {
                time::timeout(Duration::from_secs(30), async {
                    run_no_delay_worker(|context| {
                        Box::pin(async move {
                            let owner = std::thread::current().id();
                            let cases = no_delay_cases();
                            assert_eq!(cases.len(), 27, "fixed reachable default/override matrix");
                            for case in cases {
                                let actual = time::timeout(
                                    Duration::from_secs(5),
                                    observe_no_delay(&context, &case),
                                )
                                .await
                                .expect("bounded actual option observation and close");
                                assert_eq!(actual, case.expected, "kernel TCP_NODELAY: {case:?}");
                                assert_eq!(std::thread::current().id(), owner);
                            }
                            for endpoint in [
                                NoDelayEndpoint::Hostname,
                                NoDelayEndpoint::Numeric,
                                NoDelayEndpoint::BoundNumeric,
                            ] {
                                for phase in [
                                    TcpSocketOptionPhase::PreBind,
                                    TcpSocketOptionPhase::PreConnect,
                                    TcpSocketOptionPhase::PostConnect,
                                ] {
                                    if endpoint == NoDelayEndpoint::Hostname
                                        && phase != TcpSocketOptionPhase::PostConnect
                                    {
                                        continue;
                                    }
                                    time::timeout(
                                        Duration::from_secs(5),
                                        reject_no_delay_option(&context, endpoint, phase),
                                    )
                                    .await
                                    .expect("bounded strict explicit-option error and close");
                                }
                            }
                        })
                    })
                    .await;
                })
                .await
                .expect("fixed TCP option matrix and worker retirement bound");
            }));
    }

    #[cfg(unix)]
    struct NoDelayFixturePanic {
        drops: Arc<AtomicUsize>,
    }

    #[cfg(unix)]
    impl Drop for NoDelayFixturePanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg(unix)]
    #[test]
    fn tcp_no_delay_fixture_panic_joins_admitted_cleanup() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("TCP fixture failure reactor")
            .block_on(LocalSet::new().run_until(async {
                time::timeout(Duration::from_secs(5), async {
                    let owner = std::thread::current().id();
                    let completed = Arc::new(AtomicBool::new(false));
                    let drops = Arc::new(AtomicUsize::new(0));
                    let states = Arc::new(Mutex::new(Vec::new()));
                    let threads = Arc::new(Mutex::new(Vec::new()));
                    let panic_drops = Arc::new(AtomicUsize::new(0));
                    let (published_sender, published_receiver) = oneshot::channel();
                    let (closing_sender, closing_receiver) = oneshot::channel();
                    let (release_sender, mut release_receiver) = oneshot::channel();
                    let cleanup_completed = Arc::clone(&completed);
                    let cleanup_drops = Arc::clone(&drops);
                    let cleanup_states = Arc::clone(&states);
                    let cleanup_threads = Arc::clone(&threads);
                    let command_panic_drops = Arc::clone(&panic_drops);
                    let failing = AssertUnwindSafe(run_no_delay_worker(move |context| {
                        Box::pin(async move {
                            assert_eq!(context.owner_thread(), owner);
                            let cleanup_worker = context.handle().clone();
                            let marker = Rc::new(SetupDropProbe {
                                drops: cleanup_drops,
                                states: cleanup_states,
                                threads: cleanup_threads,
                                worker: cleanup_worker.clone(),
                            });
                            let (started_sender, started_receiver) = oneshot::channel();
                            let mut closing_sender = Some(closing_sender);
                            let registration = context
                                .register_cleanup_task(async move {
                                    assert_eq!(std::thread::current().id(), owner);
                                    assert_eq!(cleanup_worker.state(), WorkerState::Open);
                                    started_sender
                                        .send(())
                                        .expect("command observes cleanup start");
                                    poll_fn(|task_context| {
                                        assert_eq!(std::thread::current().id(), owner);
                                        if cleanup_worker.state() == WorkerState::Closing
                                            && let Some(sender) = closing_sender.take()
                                        {
                                            sender.send(()).expect("root observes held cleanup");
                                        }
                                        Pin::new(&mut release_receiver).poll(task_context)
                                    })
                                    .await
                                    .expect("root explicitly releases asynchronous cleanup");
                                    assert_eq!(cleanup_worker.state(), WorkerState::Closing);
                                    assert_eq!(Rc::strong_count(&marker), 1);
                                    cleanup_completed.store(true, Ordering::SeqCst);
                                    drop(marker);
                                })
                                .expect("owner-local asynchronous cleanup admitted while Open");
                            assert!(
                                published_sender
                                    .send((context.handle().clone(), registration.observe()))
                                    .is_ok(),
                                "root retains worker and actual cleanup observer"
                            );
                            drop(registration);
                            started_receiver.await.expect("cleanup is actually pending");
                            panic_any(NoDelayFixturePanic {
                                drops: command_panic_drops,
                            });
                        })
                    }))
                    .catch_unwind();
                    let release = async {
                        let (worker, observer) = published_receiver
                            .await
                            .expect("admitted cleanup publication");
                        closing_receiver
                            .await
                            .expect("normal shutdown polls the still-held cleanup");
                        assert_eq!(worker.state(), WorkerState::Closing);
                        assert!(!completed.load(Ordering::SeqCst));
                        assert_eq!(drops.load(Ordering::SeqCst), 0);
                        assert_eq!(panic_drops.load(Ordering::SeqCst), 0);
                        let mut waiting = Box::pin(observer.wait());
                        assert!(poll_once(waiting.as_mut()).is_pending());
                        drop(waiting);
                        release_sender
                            .send(())
                            .expect("cleanup retains its real gate");
                        (worker, observer)
                    };
                    let (outcome, (worker, observer)) = tokio::join!(failing, release);
                    let payload = match outcome {
                        Err(payload) => payload,
                        Ok(()) => panic!("the original command panic was swallowed"),
                    };
                    assert_eq!(worker.state(), WorkerState::Closed);
                    assert!(completed.load(Ordering::SeqCst));
                    assert_eq!(drops.load(Ordering::SeqCst), 1);
                    assert_eq!(*states.lock().unwrap(), [WorkerState::Closing]);
                    assert_eq!(*threads.lock().unwrap(), [owner]);
                    observer
                        .wait()
                        .await
                        .expect("actual cleanup future retired");
                    observer.wait().await.expect("sticky cleanup retirement");
                    let payload = payload
                        .downcast::<NoDelayFixturePanic>()
                        .unwrap_or_else(|payload| resume_unwind(payload));
                    assert!(Arc::ptr_eq(&payload.drops, &panic_drops));
                    assert_eq!(panic_drops.load(Ordering::SeqCst), 0);
                    drop(payload);
                    assert_eq!(panic_drops.load(Ordering::SeqCst), 1);
                })
                .await
                .expect("bounded original panic, actual cleanup and worker join");
            }));
    }

    #[test]
    fn connect_and_handshake_deadlines_keep_committed_timeout() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async move {
                time::pause();
                for _phase in ["connect", "handshake"] {
                    let ready = Rc::new(Cell::new(false));
                    let future_ready = Rc::clone(&ready);
                    let operation = std::future::poll_fn(move |_context| {
                        if future_ready.get() {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    });
                    let deadline = ConnectDeadline::new(Duration::from_millis(5));
                    let mut operation: Pin<Box<dyn Future<Output = Result<(), AwaitError>> + '_>> =
                        if _phase == "connect" {
                            Box::pin(await_connect_with_deadline(
                                operation,
                                &deadline,
                                CancellationToken::never(),
                            ))
                        } else {
                            Box::pin(await_handshake_with_deadline(
                                operation,
                                &deadline,
                                CancellationToken::never(),
                            ))
                        };
                    let signal = Arc::new(WakeSignal::default());
                    let waker = Waker::from(Arc::clone(&signal));
                    assert!(
                        operation
                            .as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    time::advance(Duration::from_millis(5)).await;
                    signal.wait().await;
                    ready.set(true);
                    assert_eq!(
                        operation.as_mut().poll(&mut Context::from_waker(&waker)),
                        Poll::Ready(Err(AwaitError::TimedOut)),
                    );
                }

                let deadline = ConnectDeadline::new(Duration::from_millis(5));
                assert_eq!(
                    await_connect_with_deadline(
                        std::future::ready(7),
                        &deadline,
                        CancellationToken::never(),
                    )
                    .await,
                    Ok(7),
                );
            });
    }

    #[test]
    fn tls_validation_and_transport_failures_are_distinct() {
        for (kind, code) in [
            (
                io::ErrorKind::InvalidData,
                HttpErrorCode::INADEQUATE_SECURITY,
            ),
            (io::ErrorKind::UnexpectedEof, HttpErrorCode::TRANSPORT_EOF),
            (
                io::ErrorKind::ConnectionReset,
                HttpErrorCode::TRANSPORT_READ_ERROR,
            ),
            (
                io::ErrorKind::BrokenPipe,
                HttpErrorCode::TRANSPORT_READ_ERROR,
            ),
        ] {
            assert_eq!(tls_error(io::Error::from(kind)).code(), code);
        }
    }

    #[test]
    fn tls_immediate_timed_write_preserves_idle_background_registration() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                time::pause();
                let control = CiphertextProbeControl::default();
                control.0.borrow_mut().borrowed_sends = VecDeque::from([CiphertextSend::Write(6)]);
                let transport = OwnedCiphertextStream::new(
                    control.stream([CiphertextSend::Write(3), CiphertextSend::Write(3)]),
                );
                let background = transport.background_for_test();
                let mut stream = OwnedTlsStream::new(ScriptedTlsStream {
                    transport,
                    ciphertext: b"record",
                    accepted_plaintext: 4,
                    flushes: VecDeque::new(),
                    plaintext_writes: Rc::new(Cell::new(0)),
                    wants_write: false,
                    wants_write_after_accept: false,
                    block_after_first: false,
                });
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut background_context = Context::from_waker(&waker);
                let mut context = Context::from_waker(Waker::noop());
                let data = queue(&[b"ping"]);
                assert!(background.poll(&mut background_context).is_pending());

                stream.prepare_write_timeout(Duration::from_secs(5));
                assert!(!stream.begin_owned_write(4));
                assert!(
                    !signal.take(),
                    "an idle deadline needs no background service"
                );
                assert!(matches!(
                    stream.poll_write_owned(&mut context, &data, false),
                    Poll::Ready(Ok(4)),
                ));
                assert!(!signal.take());
                assert!(!background.has_deadline());
                assert_eq!(control.0.borrow().written, b"record");

                stream.prepare_write_timeout(Duration::from_secs(5));
                assert!(!stream.begin_owned_write(4));
                assert!(!signal.take());
                assert!(matches!(
                    stream.poll_write_owned(&mut context, &data, false),
                    Poll::Ready(Ok(4)),
                ));
                assert!(signal.take(), "staging wakes the retained registration");
                assert!(background.has_deadline());
                assert!(background.poll(&mut background_context).is_pending());
                assert!(!background.has_deadline());
                assert_eq!(control.0.borrow().written, b"recordrecord");

                assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
                assert!(signal.take(), "a flush still wakes the background owner");
                assert!(background.poll(&mut background_context).is_pending());
                assert!(matches!(
                    Pin::new(&mut stream).poll_flush(&mut context),
                    Poll::Ready(Ok(())),
                ));
                assert_eq!(
                    control.0.borrow().operations,
                    ["borrowed write", "write", "write", "flush"],
                );
            });
    }

    #[test]
    fn tls_earlier_background_deadline_wakes_without_new_ciphertext() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                time::pause();
                let control = CiphertextProbeControl::default();
                let transport =
                    OwnedCiphertextStream::new(control.stream([CiphertextSend::Pending]));
                let background = transport.background_for_test();
                let mut stream = OwnedTlsStream::new(ScriptedTlsStream {
                    transport,
                    ciphertext: b"record",
                    accepted_plaintext: 4,
                    flushes: VecDeque::new(),
                    plaintext_writes: Rc::new(Cell::new(0)),
                    wants_write: false,
                    wants_write_after_accept: false,
                    block_after_first: true,
                });
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut background_context = Context::from_waker(&waker);
                let mut context = Context::from_waker(Waker::noop());
                let data = queue(&[b"ping"]);

                stream.prepare_write_timeout(Duration::from_secs(60));
                assert!(!stream.begin_owned_write(4));
                assert!(matches!(
                    stream.poll_write_owned(&mut context, &data, false),
                    Poll::Ready(Ok(4)),
                ));
                assert!(background.poll(&mut background_context).is_pending());
                assert!(!signal.take());

                stream.prepare_write_timeout(Duration::from_secs(1));
                assert!(!stream.begin_owned_write(4));
                assert!(
                    stream
                        .poll_write_owned(&mut context, &data, false)
                        .is_pending()
                );
                assert!(
                    signal.take(),
                    "the earlier deadline wakes without another enqueue"
                );
                assert!(background.poll(&mut background_context).is_pending());
                assert!(!control.0.borrow().reset);
                assert!(!signal.take());

                time::advance(Duration::from_secs(2)).await;
                time::timeout(Duration::from_secs(1), signal.wait())
                    .await
                    .expect("the replacement timer wakes without socket progress");
                assert!(background.poll(&mut background_context).is_ready());
                assert!(!background.has_deadline());
                let Poll::Ready(Err(error)) = Pin::new(&mut stream).poll_flush(&mut context) else {
                    panic!("the background timeout must remain sticky");
                };
                assert_eq!(error.kind(), io::ErrorKind::TimedOut);
                let state = control.0.borrow();
                assert!(state.reset);
                assert!(state.abandoned);
                assert!(state.written.is_empty());
            });
    }

    #[test]
    fn tls_immediate_ciphertext_write_keeps_short_write_and_staged_fifo_order() {
        let control = CiphertextProbeControl::default();
        control.0.borrow_mut().borrowed_sends = VecDeque::from([
            CiphertextSend::Write(3),
            CiphertextSend::Pending,
            CiphertextSend::Write(4),
        ]);
        let mut stream = OwnedCiphertextStream::new(control.stream([CiphertextSend::Write(9)]));
        let signal = Arc::new(WakeSignal::default());
        let background_waker = Waker::from(Arc::clone(&signal));
        assert!(
            stream
                .poll_background_for_test(&mut Context::from_waker(&background_waker))
                .is_pending()
        );
        let mut context = Context::from_waker(Waker::noop());
        let mut source = b"handshake".to_vec();
        let pointer = source.as_ptr() as usize;
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, &source),
            Poll::Ready(Ok(3)),
        ));
        assert_eq!(stream.pending_length_for_test(), 0);
        assert_eq!(control.0.borrow().written, b"han");
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, &source[3..]),
            Poll::Ready(Ok(6)),
        ));
        source.fill(b'x');
        assert!(signal.take(), "backpressure wakes the background owner");
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, b"end"),
            Poll::Ready(Ok(3)),
        ));
        assert_eq!(stream.pending_length_for_test(), 9);
        assert_eq!(control.0.borrow().borrowed_pointers, [pointer, pointer + 3]);
        assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        assert!(matches!(
            Pin::new(&mut stream).poll_flush(&mut context),
            Poll::Ready(Ok(())),
        ));
        assert_eq!(control.0.borrow().written, b"handshakeend");
        assert!(matches!(
            Pin::new(&mut stream).poll_write_vectored(
                &mut context,
                &[io::IoSlice::new(b"ne"), io::IoSlice::new(b"xt")],
            ),
            Poll::Ready(Ok(4)),
        ));
        assert_eq!(stream.pending_length_for_test(), 0);
        assert_eq!(control.0.borrow().written, b"handshakeendnext");
        assert_eq!(
            control.0.borrow().operations,
            ["borrowed write", "write", "flush", "borrowed write"],
        );
    }

    #[test]
    fn tls_immediate_ciphertext_failure_is_sticky_and_resets_transport() {
        for (send, expected) in [
            (CiphertextSend::Write(0), io::ErrorKind::WriteZero),
            (CiphertextSend::Write(9), io::ErrorKind::InvalidData),
            (
                CiphertextSend::Error(io::ErrorKind::BrokenPipe),
                io::ErrorKind::BrokenPipe,
            ),
        ] {
            let control = CiphertextProbeControl::default();
            control.0.borrow_mut().borrowed_sends.push_back(send);
            let mut stream = OwnedCiphertextStream::new(control.stream([]));
            let mut context = Context::from_waker(Waker::noop());
            for _ in 0..2 {
                let Poll::Ready(Err(error)) =
                    Pin::new(&mut stream).poll_write(&mut context, b"record")
                else {
                    panic!("ciphertext failure must remain visible");
                };
                assert_eq!(error.kind(), expected);
            }
            assert_eq!(stream.pending_length_for_test(), 0);
            assert!(control.0.borrow().reset);
            assert!(control.0.borrow().abandoned);
        }
    }

    #[test]
    fn tls_ciphertext_staging_preserves_owned_backing_fifo_and_close_order() {
        let control = CiphertextProbeControl::default();
        let mut stream = OwnedCiphertextStream::new(control.stream([
            CiphertextSend::Pending,
            CiphertextSend::Write(3),
            CiphertextSend::Write(5),
        ]));
        let mut source = b"handshake"[..8].to_vec();
        let source_pointer = source.as_ptr() as usize;
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, &source),
            Poll::Ready(Ok(8)),
        ));
        let staged = stream.pending_segments_for_test();
        let staged_pointer = staged[0].0;
        assert_ne!(staged_pointer, source_pointer);
        source.fill(b'x');
        assert_eq!(staged[0].1, b"handshak");
        assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        assert!(matches!(
            Pin::new(&mut stream).poll_flush(&mut context),
            Poll::Ready(Ok(())),
        ));
        {
            let state = control.0.borrow();
            assert_eq!(state.begin_lengths, [8]);
            assert_eq!(state.zero_copy_flags, [true, true, true]);
            assert_eq!(
                state.attempted_pointers,
                [staged_pointer, staged_pointer, staged_pointer + 3,]
            );
            assert_eq!(state.written, b"handshak");
            assert_eq!(state.operations, ["write", "write", "flush"]);
        }

        let close_control = CiphertextProbeControl::default();
        let mut close_stream = OwnedCiphertextStream::new(
            close_control.stream([CiphertextSend::Write(b"close-notify".len())]),
        );
        assert!(matches!(
            Pin::new(&mut close_stream).poll_write(&mut context, b"close-notify"),
            Poll::Ready(Ok(written)) if written == b"close-notify".len(),
        ));
        assert!(
            Pin::new(&mut close_stream)
                .poll_shutdown(&mut context)
                .is_pending()
        );
        assert!(matches!(
            close_stream.poll_background_for_test(&mut context),
            Poll::Ready(()),
        ));
        assert!(matches!(
            Pin::new(&mut close_stream).poll_shutdown(&mut context),
            Poll::Ready(Ok(())),
        ));
        assert_eq!(close_control.0.borrow().operations, ["write", "shutdown"],);

        let error_control = CiphertextProbeControl::default();
        let mut error_stream = OwnedCiphertextStream::new(
            error_control.stream([CiphertextSend::Error(io::ErrorKind::BrokenPipe)]),
        );
        assert!(matches!(
            Pin::new(&mut error_stream).poll_write(&mut context, b"alert"),
            Poll::Ready(Ok(5)),
        ));
        assert!(
            Pin::new(&mut error_stream)
                .poll_flush(&mut context)
                .is_pending()
        );
        assert!(matches!(
            error_stream.poll_background_for_test(&mut context),
            Poll::Ready(()),
        ));
        let Poll::Ready(Err(error)) = Pin::new(&mut error_stream).poll_flush(&mut context) else {
            panic!("scripted ciphertext error must be returned");
        };
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn tls_ciphertext_staging_aggregates_records_for_one_zero_copy_decision() {
        let control = CiphertextProbeControl::default();
        control.0.borrow_mut().zero_copy_threshold = 32 * 1024;
        let first = vec![b'a'; 20 * 1024];
        let second = vec![b'b'; 20 * 1024];
        let buffers = [io::IoSlice::new(&first), io::IoSlice::new(&second)];
        let mut stream = OwnedCiphertextStream::new(
            control.stream([CiphertextSend::Write(first.len() + second.len())]),
        );
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            Pin::new(&mut stream).poll_write_vectored(&mut context, &buffers),
            Poll::Ready(Ok(written)) if written == first.len() + second.len(),
        ));
        let staged = stream.pending_segments_for_test();
        assert_eq!(staged.len(), 2);
        assert_eq!(stream.pending_length_for_test(), 40 * 1024);
        assert_eq!(stream.staged_batches_for_test(), 1);
        assert_ne!(staged[0].0 as *const u8, first.as_ptr());
        assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        assert!(matches!(
            Pin::new(&mut stream).poll_flush(&mut context),
            Poll::Ready(Ok(())),
        ));
        let state = control.0.borrow();
        assert_eq!(state.begin_lengths, [40 * 1024]);
        assert_eq!(state.zero_copy_flags, [true]);
        assert_eq!(&state.written[..first.len()], first.as_slice());
        assert_eq!(&state.written[first.len()..], second.as_slice());
    }

    #[test]
    fn tls_ciphertext_survives_kernel_completion() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let controller = Rc::new(RefCell::new(TcpZeroCopy::new()));
                        let abandoned = Arc::new(AtomicBool::new(false));
                        let shutdown_complete = Arc::new(AtomicBool::new(false));
                        let lower = ZeroCopyLifecycleStream {
                            controller: Rc::clone(&controller),
                            sends: Rc::new(RefCell::new(VecDeque::from([Ok(6)]))),
                            abandoned: Arc::clone(&abandoned),
                            shutdown_complete: Arc::clone(&shutdown_complete),
                        };
                        let tls = ScriptedTlsStream {
                            transport: OwnedCiphertextStream::new(lower),
                            ciphertext: b"record",
                            accepted_plaintext: 4,
                            flushes: VecDeque::new(),
                            plaintext_writes: Rc::new(Cell::new(0)),
                            wants_write: false,
                            wants_write_after_accept: false,
                            block_after_first: false,
                        };
                        let mut stream = OwnedTlsStream::new(tls);
                        assert!(stream.enable_zero_copy(&context, 1).unwrap());
                        let completion_owner = controller.borrow().completion_owner_for_test();
                        assert_eq!(stream.zero_copy_owner_for_test(), Some(completion_owner));

                        let plaintext = queue(&[b"ping"]);
                        let mut task_context = Context::from_waker(Waker::noop());
                        assert!(!stream.begin_owned_write(plaintext.chain_length()));
                        assert!(matches!(
                            stream.poll_write_owned(&mut task_context, &plaintext, false),
                            Poll::Ready(Ok(4)),
                        ));
                        let staged = stream.pending_ciphertext_segments_for_test();
                        assert_eq!(staged.len(), 1);
                        assert_eq!(staged[0].1, b"record");
                        let staged_pointer = staged[0].0 as *const u8;
                        assert!(
                            stream
                                .poll_ciphertext_background_for_test(&mut task_context)
                                .is_pending()
                        );

                        let retained_pointer = {
                            let controller = controller.borrow();
                            assert_eq!(controller.pending_count_for_test(), 1);
                            let segments = controller.pending_segments_for_test(0);
                            assert_eq!(segments.len(), 1);
                            assert_eq!(segments[0].1, 6);
                            segments[0].0
                        };
                        assert_eq!(retained_pointer, staged_pointer);
                        assert!(stream.pending_ciphertext_segments_for_test().is_empty());
                        assert_eq!(stream.zero_copy_pending_for_test(), 1);
                        assert!(
                            Pin::new(&mut stream)
                                .poll_shutdown(&mut task_context)
                                .is_pending()
                        );
                        assert!(
                            stream
                                .poll_ciphertext_background_for_test(&mut task_context)
                                .is_pending()
                        );
                        assert!(!shutdown_complete.load(Ordering::Acquire));
                        assert_eq!(controller.borrow().pending_count_for_test(), 1);

                        assert_eq!(controller.borrow_mut().complete_for_test(1, 1, false), 0);
                        assert_eq!(controller.borrow().pending_count_for_test(), 1);
                        assert_eq!(controller.borrow_mut().complete_for_test(0, 0, false), 1);
                        assert_eq!(controller.borrow_mut().complete_for_test(0, 0, false), 0);
                        assert_eq!(stream.zero_copy_pending_for_test(), 0);
                        assert!(matches!(
                            stream.poll_ciphertext_background_for_test(&mut task_context),
                            Poll::Ready(()),
                        ));
                        assert!(matches!(
                            Pin::new(&mut stream).poll_shutdown(&mut task_context),
                            Poll::Ready(Ok(())),
                        ));
                        assert!(shutdown_complete.load(Ordering::Acquire));
                        assert!(!abandoned.load(Ordering::Acquire));
                        assert!(!TcpZeroCopy::is_quarantined_for_test(retained_pointer));

                        let cancelled_controller = Rc::new(RefCell::new(TcpZeroCopy::new()));
                        let cancelled_abandoned = Arc::new(AtomicBool::new(false));
                        let cancelled_lower = ZeroCopyLifecycleStream {
                            controller: Rc::clone(&cancelled_controller),
                            sends: Rc::new(RefCell::new(VecDeque::from([Ok(6)]))),
                            abandoned: Arc::clone(&cancelled_abandoned),
                            shutdown_complete: Arc::new(AtomicBool::new(false)),
                        };
                        let cancelled_tls = ScriptedTlsStream {
                            transport: OwnedCiphertextStream::new(cancelled_lower),
                            ciphertext: b"record",
                            accepted_plaintext: 4,
                            flushes: VecDeque::from([TlsFlushAction::Pending]),
                            plaintext_writes: Rc::new(Cell::new(0)),
                            wants_write: false,
                            wants_write_after_accept: true,
                            block_after_first: false,
                        };
                        let mut cancelled = OwnedTlsStream::new(cancelled_tls);
                        assert!(cancelled.enable_zero_copy(&context, 1).unwrap());
                        assert!(!cancelled.begin_owned_write(plaintext.chain_length()));
                        assert!(
                            cancelled
                                .poll_write_owned(&mut task_context, &plaintext, false)
                                .is_pending()
                        );
                        let cancelled_staged = cancelled.pending_ciphertext_segments_for_test();
                        assert_eq!(cancelled_staged.len(), 1);
                        assert_eq!(cancelled_staged[0].1, b"record");
                        let cancelled_staged_pointer = cancelled_staged[0].0 as *const u8;
                        assert!(
                            cancelled
                                .poll_ciphertext_background_for_test(&mut task_context)
                                .is_pending()
                        );
                        let cancelled_pointer = {
                            let controller = cancelled_controller.borrow();
                            assert_eq!(controller.pending_count_for_test(), 1);
                            controller.pending_segments_for_test(0)[0].0
                        };
                        assert_eq!(cancelled_pointer, cancelled_staged_pointer);
                        assert!(cancelled.pending_ciphertext_segments_for_test().is_empty());

                        cancelled.cancel_owned_write();
                        assert!(cancelled_controller.borrow().is_terminal_for_test());
                        assert!(cancelled_abandoned.load(Ordering::Acquire));
                        assert!(TcpZeroCopy::is_quarantined_for_test(cancelled_pointer));
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn tls_ciphertext_staging_applies_fifo_backpressure_at_128kib() {
        let control = CiphertextProbeControl::default();
        control.0.borrow_mut().zero_copy_threshold = 32 * 1024;
        let mut stream = OwnedCiphertextStream::new(control.stream([
            CiphertextSend::Write(64 * 1024),
            CiphertextSend::Write(64 * 1024),
            CiphertextSend::Write(4),
        ]));
        let full = vec![b'a'; 128 * 1024];
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, &full),
            Poll::Ready(Ok(written)) if written == full.len(),
        ));
        assert!(
            Pin::new(&mut stream)
                .poll_write(&mut context, b"next")
                .is_pending()
        );
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut context, b"next"),
            Poll::Ready(Ok(4)),
        ));
        assert!(stream.poll_background_for_test(&mut context).is_pending());
        let state = control.0.borrow();
        assert_eq!(state.begin_lengths, [128 * 1024, 4]);
        assert_eq!(state.zero_copy_flags, [true, true, false]);
        assert_eq!(&state.written[..full.len()], full.as_slice());
        assert_eq!(&state.written[full.len()..], b"next");
    }

    #[test]
    fn tls_background_failure_wakes_distinct_read_and_write_waiters() {
        let control = CiphertextProbeControl::default();
        let mut stream = OwnedCiphertextStream::new(
            control.stream([CiphertextSend::Error(io::ErrorKind::BrokenPipe)]),
        );
        let full = vec![b'x'; 128 * 1024];
        let mut neutral_context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut neutral_context, &full),
            Poll::Ready(Ok(written)) if written == full.len(),
        ));

        let read_signal = Arc::new(WakeSignal::default());
        let read_waker = Waker::from(Arc::clone(&read_signal));
        let mut read_context = Context::from_waker(&read_waker);
        let mut bytes = [0; 1];
        let mut read_buffer = ReadBuf::new(&mut bytes);
        stream.set_read_role_for_test();
        assert!(
            Pin::new(&mut stream)
                .poll_read(&mut read_context, &mut read_buffer)
                .is_pending()
        );

        let write_signal = Arc::new(WakeSignal::default());
        let write_waker = Waker::from(Arc::clone(&write_signal));
        let mut write_context = Context::from_waker(&write_waker);
        stream.set_write_role_for_test();
        assert!(
            Pin::new(&mut stream)
                .poll_write(&mut write_context, b"blocked")
                .is_pending()
        );

        assert!(matches!(
            stream.poll_background_for_test(&mut neutral_context),
            Poll::Ready(()),
        ));
        assert!(read_signal.take());
        assert!(write_signal.take());
    }

    #[test]
    fn tls_cancelled_read_releases_its_registered_waker() {
        let control = CiphertextProbeControl::default();
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"record",
            accepted_plaintext: 4,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let weak = {
            let signal = Arc::new(WakeSignal::default());
            let weak = Arc::downgrade(&signal);
            let waker = Waker::from(signal);
            let mut context = Context::from_waker(&waker);
            let mut bytes = [0; 1];
            let mut buffer = ReadBuf::new(&mut bytes);
            assert!(
                Pin::new(&mut stream)
                    .poll_read(&mut context, &mut buffer)
                    .is_pending()
            );
            weak
        };
        assert!(weak.upgrade().is_some());
        TokioByteStream::cancel_read(&mut stream);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn tls_owned_read_pending_does_not_allocate_and_cancel_releases_waker() {
        let control = CiphertextProbeControl::default();
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"record",
            accepted_plaintext: 4,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let mut read = OwnedRead::new(256 * 1024);
        let weak = {
            let signal = Arc::new(WakeSignal::default());
            let weak = Arc::downgrade(&signal);
            let waker = Waker::from(signal);
            let mut context = Context::from_waker(&waker);
            assert!(
                stream
                    .poll_read_owned(&mut context, 256 * 1024, 256 * 1024, &mut read)
                    .is_pending()
            );
            weak
        };
        assert!(read.is_empty());
        assert_eq!(read.held_capacity_for_test(), 0);
        assert!(weak.upgrade().is_some());
        TokioByteStream::cancel_read(&mut stream);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn tls_plaintext_progress_waits_for_ciphertext_and_uses_one_tcp_owner() {
        let control = CiphertextProbeControl::default();
        let plaintext_writes = Rc::new(Cell::new(0));
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream([CiphertextSend::Write(b"ciphertext".len())]),
            ),
            ciphertext: b"ciphertext",
            accepted_plaintext: 5,
            flushes: VecDeque::from([TlsFlushAction::Pending, TlsFlushAction::Ready]),
            plaintext_writes: Rc::clone(&plaintext_writes),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let plaintext = b"hello".to_vec();
        let plaintext_pointer = plaintext.as_ptr() as usize;
        let data = BufQueue::from_bytes(plaintext.into(), 5).expect("plaintext fits");
        let mut context = Context::from_waker(Waker::noop());

        assert!(!stream.begin_owned_write(5));
        assert!(matches!(
            stream.poll_write_owned(&mut context, &data, false),
            Poll::Ready(Ok(5)),
        ));
        assert_eq!(plaintext_writes.get(), 1);
        assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
        assert!(Pin::new(&mut stream).poll_flush(&mut context).is_pending());
        assert!(
            stream
                .poll_ciphertext_background_for_test(&mut context)
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut stream).poll_flush(&mut context),
            Poll::Ready(Ok(())),
        ));
        assert_eq!(TokioByteStream::zero_copy_owner_for_test(&stream), Some(7));
        {
            let state = control.0.borrow();
            assert_eq!(state.begin_lengths, [b"ciphertext".len()]);
            assert_eq!(state.zero_copy_flags, [true]);
            assert_eq!(state.written, b"ciphertext");
            assert_ne!(state.attempted_pointers[0], plaintext_pointer);
        }
        stream.cancel_owned_write();
        assert!(!stream.is_poisoned_for_test());
    }

    #[test]
    fn tls_plaintext_progress_waits_only_for_remaining_rustls_output() {
        let control = CiphertextProbeControl::default();
        let plaintext_writes = Rc::new(Cell::new(0));
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream([CiphertextSend::Write(b"ciphertext".len())]),
            ),
            ciphertext: b"ciphertext",
            accepted_plaintext: 5,
            flushes: VecDeque::from([TlsFlushAction::Pending, TlsFlushAction::Ready]),
            plaintext_writes: Rc::clone(&plaintext_writes),
            wants_write: false,
            wants_write_after_accept: true,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let data = BufQueue::copy_from_slice(b"hello", 5).expect("plaintext fits");
        let mut context = Context::from_waker(Waker::noop());

        assert!(!stream.begin_owned_write(5));
        assert!(
            stream
                .poll_write_owned(&mut context, &data, false)
                .is_pending()
        );
        assert_eq!(plaintext_writes.get(), 1);
        assert!(matches!(
            stream.poll_write_owned(&mut context, &data, false),
            Poll::Ready(Ok(5)),
        ));
        assert_eq!(plaintext_writes.get(), 1);
        assert!(
            stream
                .poll_ciphertext_background_for_test(&mut context)
                .is_pending()
        );
        assert_eq!(control.0.borrow().written, b"ciphertext");
    }

    #[test]
    fn tls_pending_plaintext_does_not_hide_lower_flush_error() {
        let control = CiphertextProbeControl::default();
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"ciphertext",
            accepted_plaintext: 5,
            flushes: VecDeque::from([TlsFlushAction::Error(io::ErrorKind::BrokenPipe)]),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: true,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let data = BufQueue::copy_from_slice(b"hello", 5).expect("plaintext fits");
        let mut context = Context::from_waker(Waker::noop());

        assert!(!stream.begin_owned_write(5));
        let Poll::Ready(Err(error)) = stream.poll_write_owned(&mut context, &data, false) else {
            panic!("lower flush error must precede retained plaintext progress");
        };
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(stream.is_poisoned_for_test());
        let state = control.0.borrow();
        assert!(state.reset);
        assert!(state.abandoned);
    }

    #[test]
    fn tls_transport_abandon_is_non_abortive_but_cancelled_write_resets() {
        let abandoned_control = CiphertextProbeControl::default();
        let abandoned_tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                abandoned_control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"",
            accepted_plaintext: 0,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut abandoned = OwnedTlsStream::new(abandoned_tls);

        abandoned.abandon_zero_copy();

        let state = abandoned_control.0.borrow();
        assert!(state.abandoned);
        assert!(!state.reset);
        drop(state);

        let cancelled_control = CiphertextProbeControl::default();
        let cancelled_tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                cancelled_control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"",
            accepted_plaintext: 0,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut cancelled = OwnedTlsStream::new(cancelled_tls);
        assert!(!cancelled.begin_owned_write(1));

        cancelled.cancel_owned_write();

        let state = cancelled_control.0.borrow();
        assert!(state.abandoned);
        assert!(state.reset);
    }

    #[test]
    fn tls_background_failure_follows_reported_plaintext_success() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let control = CiphertextProbeControl::default();
                        let transport =
                            OwnedCiphertextStream::new(control.stream([CiphertextSend::Pending]));
                        let background = transport.background_for_test();
                        let background_task = {
                            let background = background.clone();
                            tokio::task::spawn_local(async move {
                                std::future::poll_fn(|task_context| background.poll(task_context))
                                    .await;
                            })
                        };
                        let tls = ScriptedTlsStream {
                            transport,
                            ciphertext: b"record",
                            accepted_plaintext: 4,
                            flushes: VecDeque::new(),
                            plaintext_writes: Rc::new(Cell::new(0)),
                            wants_write: false,
                            wants_write_after_accept: false,
                            block_after_first: false,
                        };
                        let stream = owned_from_stream(
                            &context,
                            OwnedTlsStream::new(tls),
                            SocketAddress::default(),
                            SocketAddress::default(),
                            None,
                            limits(),
                        )
                        .unwrap();

                        let first = stream
                            .write(
                                &context,
                                queue(&[b"warm"]),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .expect("zero-timeout TLS plaintext stages successfully");
                        assert_eq!(first.bytes_written, 4);
                        tokio::task::yield_now().await;
                        let second = stream
                            .write(
                                &context,
                                queue(&[b"ping"]),
                                Duration::from_millis(20),
                                CancellationToken::never(),
                            )
                            .await
                            .expect("small TLS plaintext succeeds once ciphertext is staged");
                        assert_eq!(second.bytes_written, 4);
                        assert!(background.has_deadline());
                        let read =
                            stream.read(&context, 1, 1, Duration::ZERO, CancellationToken::never());
                        tokio::pin!(read);
                        std::future::poll_fn(|task_context| {
                            match read.as_mut().poll(task_context) {
                                Poll::Pending => Poll::Ready(()),
                                Poll::Ready(result) => {
                                    panic!("read completed before background timeout: {result:?}")
                                }
                            }
                        })
                        .await;
                        background_task.await.unwrap();
                        let read_error = read.await.expect_err("background timeout wakes the read");
                        assert_ne!(read_error.code(), HttpErrorCode::CORO_CANCELLED);
                        let error = stream
                            .write(
                                &context,
                                queue(&[b"x"]),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .expect_err("background timeout poisons the next operation");
                        assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                        assert_eq!(error.bytes_written(), 0);
                        let state = control.0.borrow();
                        assert!(state.reset);
                        assert!(state.abandoned);
                        assert!(state.written.is_empty());
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn tls_reported_consecutive_writes_drain_after_tcp_unblocks() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let control = CiphertextProbeControl::default();
                let transport =
                    OwnedCiphertextStream::new(control.stream([CiphertextSend::Pending]));
                let background = transport.background_for_test();
                let background_task = tokio::task::spawn_local(async move {
                    std::future::poll_fn(|task_context| background.poll(task_context)).await;
                });
                let tls = ScriptedTlsStream {
                    transport,
                    ciphertext: b"record",
                    accepted_plaintext: 4,
                    flushes: VecDeque::new(),
                    plaintext_writes: Rc::new(Cell::new(0)),
                    wants_write: false,
                    wants_write_after_accept: false,
                    block_after_first: false,
                };
                let mut stream = OwnedTlsStream::new(tls);
                let mut context = Context::from_waker(Waker::noop());
                for plaintext in [b"one!".as_slice(), b"two!".as_slice()] {
                    let data = BufQueue::copy_from_slice(plaintext, 4).expect("plaintext fits");
                    assert!(!stream.begin_owned_write(4));
                    assert!(matches!(
                        stream.poll_write_owned(&mut context, &data, false),
                        Poll::Ready(Ok(4)),
                    ));
                }
                tokio::task::yield_now().await;
                assert!(control.0.borrow().written.is_empty());

                control.allow_send(12);
                time::timeout(Duration::from_secs(1), control.wait_written(12))
                    .await
                    .expect("queued ciphertext drains after TCP becomes writable");
                assert_eq!(control.0.borrow().written, b"recordrecord");
                drop(stream);
                background_task.await.unwrap();
            }));
    }

    #[test]
    fn tls_full_staging_blocks_plaintext_and_cancellation_resets() {
        let control = CiphertextProbeControl::default();
        let mut transport =
            OwnedCiphertextStream::new(control.stream(std::iter::empty::<CiphertextSend>()));
        let full = vec![b'x'; 128 * 1024];
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(&mut transport).poll_write(&mut context, &full),
            Poll::Ready(Ok(written)) if written == full.len(),
        ));
        let tls = ScriptedTlsStream {
            transport,
            ciphertext: b"record",
            accepted_plaintext: 4,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let data = BufQueue::copy_from_slice(b"ping", 4).expect("plaintext fits");

        assert!(!stream.begin_owned_write(4));
        assert!(
            stream
                .poll_write_owned(&mut context, &data, false)
                .is_pending()
        );
        stream.cancel_owned_write();
        assert!(stream.is_poisoned_for_test());
        let state = control.0.borrow();
        assert!(state.reset);
        assert!(state.abandoned);
        assert!(state.written.is_empty());
    }

    #[test]
    fn tls_same_write_cancellation_reports_only_completed_plaintext_prefix() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let control = CiphertextProbeControl::default();
                        let tls = ScriptedTlsStream {
                            transport: OwnedCiphertextStream::new(
                                control.stream(std::iter::empty::<CiphertextSend>()),
                            ),
                            ciphertext: b"record",
                            accepted_plaintext: 4,
                            flushes: VecDeque::new(),
                            plaintext_writes: Rc::new(Cell::new(0)),
                            wants_write: false,
                            wants_write_after_accept: false,
                            block_after_first: true,
                        };
                        let stream = owned_from_stream(
                            &context,
                            OwnedTlsStream::new(tls),
                            SocketAddress::default(),
                            SocketAddress::default(),
                            None,
                            limits(),
                        )
                        .unwrap();
                        let cancellation = CancellationSource::new();
                        let write = stream.write(
                            &context,
                            queue(&[b"abcdefgh"]),
                            Duration::ZERO,
                            cancellation.token(),
                        );
                        tokio::pin!(write);
                        std::future::poll_fn(|task_context| {
                            match write.as_mut().poll(task_context) {
                                Poll::Pending => Poll::Ready(()),
                                Poll::Ready(result) => {
                                    panic!(
                                        "second plaintext prefix must remain blocked: {result:?}"
                                    )
                                }
                            }
                        })
                        .await;
                        assert!(cancellation.request_cancellation());
                        let error = write.await.expect_err("blocked TLS write is cancelled");
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                        assert_eq!(error.bytes_written(), 4);
                        let state = control.0.borrow();
                        assert!(state.reset);
                        assert!(state.abandoned);
                        assert!(state.written.is_empty());
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn tls_nonempty_zero_plaintext_progress_poison_resets() {
        let control = CiphertextProbeControl::default();
        let tls = ScriptedTlsStream {
            transport: OwnedCiphertextStream::new(
                control.stream(std::iter::empty::<CiphertextSend>()),
            ),
            ciphertext: b"record",
            accepted_plaintext: 0,
            flushes: VecDeque::new(),
            plaintext_writes: Rc::new(Cell::new(0)),
            wants_write: false,
            wants_write_after_accept: false,
            block_after_first: false,
        };
        let mut stream = OwnedTlsStream::new(tls);
        let data = BufQueue::copy_from_slice(b"ping", 4).expect("plaintext fits");
        let mut context = Context::from_waker(Waker::noop());

        assert!(!stream.begin_owned_write(4));
        let Poll::Ready(Err(error)) = stream.poll_write_owned(&mut context, &data, false) else {
            panic!("nonempty TLS zero progress must fail");
        };
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert!(stream.is_poisoned_for_test());
        let state = control.0.borrow();
        assert!(state.reset);
        assert!(state.abandoned);
    }

    #[test]
    fn tls_unsupported_zero_copy_delegation_keeps_writes_usable() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let control = CiphertextProbeControl::default();
                        let tls = ScriptedTlsStream {
                            transport: OwnedCiphertextStream::new(
                                control.stream([CiphertextSend::Write(6)]),
                            ),
                            ciphertext: b"record",
                            accepted_plaintext: 4,
                            flushes: VecDeque::new(),
                            plaintext_writes: Rc::new(Cell::new(0)),
                            wants_write: false,
                            wants_write_after_accept: false,
                            block_after_first: false,
                        };
                        let mut stream = OwnedTlsStream::new(tls);
                        assert!(!stream.enable_zero_copy(&context, 1).unwrap());
                        let data = BufQueue::copy_from_slice(b"ping", 4).expect("plaintext fits");
                        let mut task_context = Context::from_waker(Waker::noop());
                        assert!(!stream.begin_owned_write(4));
                        assert!(matches!(
                            stream.poll_write_owned(&mut task_context, &data, false),
                            Poll::Ready(Ok(4)),
                        ));
                        assert!(
                            stream
                                .poll_ciphertext_background_for_test(&mut task_context)
                                .is_pending()
                        );
                        assert_eq!(control.0.borrow().written, b"record");
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn owned_byte_stream_applies_tx_ack_write_flags_once() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let delivered_control = DrainControl::default();
                            delivered_control.allow_write();
                            let delivered_events = CapableEventControl::default();
                            let delivered = owned_from_stream(
                                &context,
                                CapableDrainStream {
                                    inner: DrainStream {
                                        control: delivered_control,
                                        worker: stream_worker.clone(),
                                    },
                                    events: delivered_events.clone(),
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let delivered_log = Rc::new(TransportEventLog::default());
                            delivered
                                .register_byte_event(
                                    &context,
                                    HttpByteEventRegistration::new(
                                        None,
                                        ByteEventType::NicTx.bit()
                                            | ByteEventType::CumulativeAck.bit(),
                                        &delivered_log,
                                    ),
                                )
                                .unwrap();
                            assert_eq!(
                                delivered
                                    .write_with_byte_events(
                                        &context,
                                        queue(&[b"ping"]),
                                        ByteEventWriteFlags {
                                            timestamp_write: true,
                                            timestamp_tx: true,
                                            timestamp_ack: true,
                                        },
                                        Duration::ZERO,
                                        CancellationToken::never(),
                                    )
                                    .await
                                    .unwrap()
                                    .bytes_written,
                                4,
                            );
                            assert!(delivered_log.fired.borrow().is_empty());
                            assert_eq!(
                                delivered_events.flags.borrow().as_slice(),
                                &[ByteEventWriteFlags {
                                    timestamp_write: true,
                                    timestamp_tx: true,
                                    timestamp_ack: true,
                                }],
                            );
                            delivered_events.notify(ByteEventType::NicTx, 4);
                            assert_eq!(delivered_log.fired.borrow().len(), 1);
                            delivered_events.notify(ByteEventType::NicTx, 4);
                            delivered_events.notify(ByteEventType::CumulativeAck, 4);
                            delivered_events.notify(ByteEventType::CumulativeAck, 4);
                            assert_eq!(
                                delivered_log
                                    .fired
                                    .borrow()
                                    .iter()
                                    .map(|event| (event.event_type, event.transport_offset))
                                    .collect::<Vec<_>>(),
                                vec![(ByteEventType::NicTx, 4), (ByteEventType::CumulativeAck, 4),],
                            );
                            assert!(delivered_log.cancelled.borrow().is_empty());
                            drop(delivered);
                            assert_eq!(delivered_log.fired.borrow().len(), 2);

                            let cancelled_control = DrainControl::default();
                            cancelled_control.allow_write();
                            let cancelled_events = CapableEventControl::default();
                            let cancelled = owned_from_stream(
                                &context,
                                CapableDrainStream {
                                    inner: DrainStream {
                                        control: cancelled_control,
                                        worker: stream_worker,
                                    },
                                    events: cancelled_events.clone(),
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let cancelled_log = Rc::new(TransportEventLog::default());
                            cancelled
                                .register_byte_event(
                                    &context,
                                    HttpByteEventRegistration::new(
                                        None,
                                        ByteEventType::NicTx.bit()
                                            | ByteEventType::CumulativeAck.bit(),
                                        &cancelled_log,
                                    ),
                                )
                                .unwrap();
                            cancelled
                                .write_with_byte_events(
                                    &context,
                                    queue(&[b"pending"]),
                                    ByteEventWriteFlags {
                                        timestamp_write: true,
                                        timestamp_tx: true,
                                        timestamp_ack: true,
                                    },
                                    Duration::ZERO,
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            cancelled_events.notify(ByteEventType::NicTx, 7);
                            assert_eq!(cancelled_log.fired.borrow().len(), 1);
                            cancelled.close_with_reset(&context).unwrap();
                            assert_eq!(cancelled_log.cancelled.borrow().len(), 1);
                            assert_eq!(
                                cancelled_log.cancelled.borrow()[0].0.event_type,
                                ByteEventType::CumulativeAck,
                            );
                            assert_eq!(
                                cancelled_log.cancelled.borrow()[0].1,
                                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            );
                            drop(cancelled);
                            assert_eq!(cancelled_log.fired.borrow().len(), 1);
                            assert_eq!(cancelled_log.cancelled.borrow().len(), 1);
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                })
                .await
                .expect("production byte-event bridge must remain bounded");
            }));
    }

    #[test]
    fn pending_half_close_drop_cancels_events_but_keeps_drain() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let control = DrainControl::default();
                            control.allow_write();
                            let events = CapableEventControl::default();
                            let transport = owned_from_stream(
                                &context,
                                CapableDrainStream {
                                    inner: DrainStream {
                                        control: control.clone(),
                                        worker: stream_worker,
                                    },
                                    events: events.clone(),
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let log = Rc::new(TransportEventLog::default());
                            transport
                                .register_byte_event(
                                    &context,
                                    HttpByteEventRegistration::new(
                                        None,
                                        ByteEventType::CumulativeAck.bit(),
                                        &log,
                                    ),
                                )
                                .unwrap();
                            transport
                                .write_with_byte_events(
                                    &context,
                                    queue(&[b"done"]),
                                    ByteEventWriteFlags {
                                        timestamp_ack: true,
                                        ..Default::default()
                                    },
                                    Duration::ZERO,
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            transport.shutdown_write(&context).unwrap();
                            control
                                .wait_for_action(|action| {
                                    matches!(action, DrainAction::ShutdownPending(..))
                                })
                                .await;
                            assert!(log.cancelled.borrow().is_empty());
                            drop(transport);
                            assert_eq!(log.cancelled.borrow().len(), 1);
                            assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::DROPPED);
                            assert!(
                                !control
                                    .actions()
                                    .iter()
                                    .any(|action| matches!(action, DrainAction::Drop(..)))
                            );
                            control.permit_shutdowns(1);
                            control
                                .wait_for_action(|action| matches!(action, DrainAction::Drop(..)))
                                .await;
                            assert!(
                                control
                                    .actions()
                                    .iter()
                                    .any(|action| matches!(action, DrainAction::ShutdownReady(..)))
                            );
                            events.notify(ByteEventType::CumulativeAck, 4);
                            assert!(log.fired.borrow().is_empty());
                            assert_eq!(log.cancelled.borrow().len(), 1);
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                })
                .await
                .expect("dropped half-close drain completes within its bound");
            }));
    }

    #[test]
    fn graceful_close_drain_is_worker_owned_and_shutdown_safe() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let control = DrainControl::default();
                    let call_control = control.clone();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let events = CapableEventControl::default();
                            let transport = owned_from_stream(
                                &context,
                                CapableDrainStream {
                                    inner: DrainStream {
                                        control: call_control.clone(),
                                        worker: stream_worker,
                                    },
                                    events: events.clone(),
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let log = Rc::new(TransportEventLog::default());
                            transport
                                .register_byte_event(
                                    &context,
                                    HttpByteEventRegistration::new(
                                        None,
                                        ByteEventType::CumulativeAck.bit(),
                                        &log,
                                    ),
                                )
                                .unwrap();
                            let mut write = transport.write_with_byte_events(
                                &context,
                                queue(&[b"drain"]),
                                ByteEventWriteFlags {
                                    timestamp_ack: true,
                                    ..Default::default()
                                },
                                Duration::ZERO,
                                CancellationToken::never(),
                            );
                            assert!(poll_once(write.as_mut()).is_pending());
                            transport.close(&context).unwrap();
                            assert!(call_control.actions().is_empty());
                            assert!(log.cancelled.borrow().is_empty());
                            drop(transport);
                            assert_eq!(log.cancelled.borrow().len(), 1);
                            assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::DROPPED);
                            call_control.permit_shutdowns(1);
                            call_control.allow_write();
                            assert_eq!(write.await.unwrap().bytes_written, 5);
                            call_control
                                .wait_for_action(|action| matches!(action, DrainAction::Drop(..)))
                                .await;
                            assert_eq!(
                                call_control.actions(),
                                vec![
                                    DrainAction::Write(owner),
                                    DrainAction::ShutdownReady(owner),
                                    DrainAction::Drop(WorkerState::Open, owner),
                                ],
                            );
                            events.notify(ByteEventType::CumulativeAck, 5);
                            assert!(log.fired.borrow().is_empty());
                            assert_eq!(log.cancelled.borrow().len(), 1);
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    let completed = control.actions();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    tokio::task::yield_now().await;
                    assert_eq!(control.actions(), completed);
                })
                .await
                .expect("normal graceful close drain must remain bounded");
            }));

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let control = DrainControl::default();
                    let call_control = control.clone();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport = owned_from_stream(
                                &context,
                                DrainStream {
                                    control: call_control.clone(),
                                    worker: stream_worker,
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            transport.close(&context).unwrap();
                            call_control
                                .wait_for_action(|action| {
                                    matches!(action, DrainAction::ShutdownPending(_))
                                })
                                .await;
                            ESCAPED_DRAIN.with(|escaped| {
                                assert!(escaped.replace(Some(transport)).is_none());
                            });
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    let closed = control.actions();
                    assert_eq!(
                        closed
                            .iter()
                            .filter(|action| matches!(action, DrainAction::Drop(..)))
                            .count(),
                        1,
                    );
                    assert_eq!(
                        closed.last(),
                        Some(&DrainAction::Drop(WorkerState::Closing, owner)),
                    );
                    assert!(closed.iter().all(|action| match action {
                        DrainAction::Write(thread)
                        | DrainAction::ShutdownReady(thread)
                        | DrainAction::ShutdownPending(thread)
                        | DrainAction::Drop(_, thread) => *thread == owner,
                    }));
                    ESCAPED_DRAIN.with(|escaped| drop(escaped.replace(None)));
                    tokio::task::yield_now().await;
                    assert_eq!(control.actions(), closed);
                })
                .await
                .expect("shutdown during graceful drain must remain bounded");
            }));
    }

    #[test]
    fn shutdown_write_drain_outlives_transport_handle() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let control = DrainControl::default();
                    let call_control = control.clone();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport = owned_from_stream(
                                &context,
                                DrainStream {
                                    control: call_control.clone(),
                                    worker: stream_worker,
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            transport.shutdown_write(&context).unwrap();
                            call_control
                                .wait_for_action(|action| {
                                    matches!(action, DrainAction::ShutdownPending(_))
                                })
                                .await;

                            drop(transport);
                            assert!(
                                !call_control
                                    .actions()
                                    .iter()
                                    .any(|action| matches!(action, DrainAction::Drop(..))),
                            );

                            call_control.permit_shutdowns(1);
                            call_control
                                .wait_for_action(|action| matches!(action, DrainAction::Drop(..)))
                                .await;
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    assert_eq!(
                        control.actions(),
                        vec![
                            DrainAction::ShutdownPending(owner),
                            DrainAction::ShutdownReady(owner),
                            DrainAction::Drop(WorkerState::Open, owner),
                        ],
                    );
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                })
                .await
                .expect("detached graceful shutdown must remain bounded");
            }));
    }

    #[test]
    fn shutdown_drain_records_guard_completion_and_repolls_half_close() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let control = DrainControl::default();
                    let call_control = control.clone();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport = owned_from_stream(
                                &context,
                                DrainStream {
                                    control: call_control.clone(),
                                    worker: stream_worker,
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let mut write = transport.write(
                                &context,
                                queue(&[b"error"]),
                                Duration::ZERO,
                                CancellationToken::never(),
                            );
                            assert!(poll_once(write.as_mut()).is_pending());
                            transport.close(&context).unwrap();
                            call_control.permit_shutdowns(1);
                            call_control.fail_write();
                            assert_eq!(
                                write.await.err().unwrap().code(),
                                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            );
                            call_control
                                .wait_for_action(|action| matches!(action, DrainAction::Drop(..)))
                                .await;
                            assert_eq!(
                                call_control.actions(),
                                vec![
                                    DrainAction::Write(owner),
                                    DrainAction::ShutdownReady(owner),
                                    DrainAction::Drop(WorkerState::Open, owner),
                                ],
                            );
                            drop(transport);
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                })
                .await
                .expect("guard-completed shutdown drain must remain bounded");
            }));

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let control = DrainControl::default();
                    let call_control = control.clone();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport = owned_from_stream(
                                &context,
                                DrainStream {
                                    control: call_control.clone(),
                                    worker: stream_worker,
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            transport.shutdown_write(&context).unwrap();
                            call_control
                                .wait_for_action(|action| {
                                    matches!(action, DrainAction::ShutdownPending(_))
                                })
                                .await;
                            call_control.permit_shutdowns(1);
                            call_control
                                .wait_for_action(|action| {
                                    matches!(action, DrainAction::ShutdownReady(_))
                                })
                                .await;
                            let before_drop = call_control.actions();
                            assert!(before_drop.iter().any(|action| {
                                matches!(action, DrainAction::ShutdownPending(_))
                            }));
                            assert_eq!(
                                before_drop
                                    .iter()
                                    .filter(|action| {
                                        matches!(action, DrainAction::ShutdownReady(_))
                                    })
                                    .count(),
                                1,
                            );
                            assert!(
                                !before_drop
                                    .iter()
                                    .any(|action| matches!(action, DrainAction::Drop(..)))
                            );
                            drop(transport);
                            let after_drop = call_control.actions();
                            assert_eq!(
                                after_drop
                                    .iter()
                                    .filter(|action| matches!(action, DrainAction::Drop(..)))
                                    .count(),
                                1,
                            );
                            assert_eq!(
                                after_drop.last(),
                                Some(&DrainAction::Drop(WorkerState::Open, owner)),
                            );
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                })
                .await
                .expect("pending half-close drain must remain bounded");
            }));
    }

    #[test]
    fn tcp_read_preserves_allocation() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(|context| async move {
                        let read_allocations = Rc::new(RefCell::new(Vec::new()));
                        let stream = BackingProbeStream {
                            read_payload: Box::from(&b"abcdef"[..]),
                            read_offset: 0,
                            read_allocations: Rc::clone(&read_allocations),
                            write_pointers: Rc::new(RefCell::new(Vec::new())),
                        };
                        let transport = owned_from_stream(
                            &context,
                            stream,
                            SocketAddress::new("127.0.0.1", 1000),
                            SocketAddress::new("127.0.0.1", 2000),
                            None,
                            limits(),
                        )
                        .expect("probe transport is adopted");

                        let read = transport
                            .read(&context, 6, 3, Duration::ZERO, CancellationToken::never())
                            .await
                            .expect("probe read succeeds");
                        assert_eq!(read_bytes_ref(&read), b"abcdef");
                        let allocations = read_allocations.borrow().clone();
                        assert_eq!(
                            allocations.len(),
                            2,
                            "a 3-byte first buffer fills, so the read offers 3 more bytes",
                        );
                        let segments = read
                            .data()
                            .segments()
                            .map(|segment| segment.as_ptr() as usize)
                            .collect::<Vec<_>>();
                        assert_eq!(segments.len(), 2);
                        assert_ne!(
                            segments[0], allocations[0],
                            "the first 3 bytes are an exact-size copy out of the staging buffer",
                        );
                        assert_eq!(
                            segments[1], allocations[1],
                            "the continuation segment is the buffer the stream filled",
                        );
                        assert_eq!(
                            read.data().backing_capacity_upper_bound(),
                            Some(6),
                            "the 3-byte copy and the 3-byte continuation buffer hold 6 bytes",
                        );

                        transport.close(&context).expect("transport closes");
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    /// Waits until `length` bytes are queued on `socket`, a clone of a
    /// nonblocking reading socket, because a sender's `write_all` does not
    /// mean the receiver can already read every byte.
    async fn wait_until_queued(socket: &std::net::TcpStream, length: usize) {
        let mut queued = vec![0; length];
        loop {
            match socket.peek(&mut queued) {
                Ok(peeked) if peeked >= length => return,
                Ok(_) => {}
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}"),
            }
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn tcp_read_starts_at_allocation_size_and_grows_toward_maximum() {
        const ALLOCATION_SIZE: usize = 1_000;
        const MAXIMUM: usize = 8_000;
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let peer = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                peer.set_nodelay(true).unwrap();
                let (accepted, _) = listener.accept().await.unwrap();
                let (accepted, peer) = (accepted.into_std().unwrap(), peer.into_std().unwrap());
                let observer = accepted.try_clone().unwrap();
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let polls = Rc::new(RefCell::new(Vec::new()));
                        let transport = owned_from_stream(
                            &context,
                            ReadPollProbe {
                                stream: TcpStream::from_std(accepted).unwrap(),
                                polls: Rc::clone(&polls),
                            },
                            SocketAddress::new("127.0.0.1", 1000),
                            SocketAddress::new("127.0.0.1", 2000),
                            None,
                            ByteStreamLimits::new(64 * 1024, 1024, 8).unwrap(),
                        )
                        .unwrap();
                        let mut peer = TcpStream::from_std(peer).unwrap();
                        let read = || {
                            transport.read(
                                &context,
                                MAXIMUM,
                                ALLOCATION_SIZE,
                                Duration::from_secs(5),
                                CancellationToken::never(),
                            )
                        };

                        let mut idle = read();
                        assert!(poll_once(idle.as_mut()).is_pending());
                        assert_eq!(
                            polls.take(),
                            vec![ReadPoll {
                                offered: 1_000,
                                filled: None,
                            }],
                            "an idle read offers only allocation_size bytes",
                        );

                        let burst = pattern(5_000, 1);
                        peer.write_all(&burst).await.unwrap();
                        wait_until_queued(&observer, burst.len()).await;
                        let response = idle.await.unwrap();
                        assert_eq!(response.bytes_read(), burst.len());
                        assert!(read_bytes(response) == burst);
                        assert_eq!(
                            data_polls(polls.take(), ALLOCATION_SIZE),
                            vec![
                                (1_000, 1_000),
                                (1_000, 1_000),
                                (2_000, 2_000),
                                (4_000, 1_000)
                            ],
                            "each filled poll doubles the gathered capacity, so a burst \
                             between allocation_size and maximum takes one read",
                        );

                        let burst = pattern(11_000, 2);
                        peer.write_all(&burst).await.unwrap();
                        wait_until_queued(&observer, burst.len()).await;
                        let first = read().await.unwrap();
                        assert_eq!(first.bytes_read(), MAXIMUM);
                        assert_eq!(
                            data_polls(polls.take(), ALLOCATION_SIZE),
                            vec![
                                (1_000, 1_000),
                                (1_000, 1_000),
                                (2_000, 2_000),
                                (4_000, 4_000)
                            ],
                            "growth stops once the read holds maximum bytes",
                        );
                        let rest = read().await.unwrap();
                        assert_eq!(
                            data_polls(polls.take(), ALLOCATION_SIZE),
                            vec![(1_000, 1_000), (1_000, 1_000), (2_000, 1_000)],
                            "the next read starts again at allocation_size",
                        );
                        let mut received = read_bytes(first);
                        received.extend(read_bytes(rest));
                        assert!(received == burst);

                        transport.close(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("TCP read growth remains bounded")
                    .unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn tcp_read_spends_one_coop_budget_unit_and_still_batches() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let mut peer = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (mut accepted, _) = listener.accept().await.unwrap();
                let burst = pattern(5_000, 6);
                peer.write_all(&burst).await.unwrap();
                let mut queued = vec![0; burst.len()];
                while accepted.peek(&mut queued).await.unwrap() < burst.len() {
                    tokio::task::yield_now().await;
                }

                // Measure a fresh budget, then leave exactly one unit of the next.
                tokio::task::yield_now().await;
                let mut budget = 0;
                while coop::has_budget_remaining() {
                    coop::consume_budget().await;
                    budget += 1;
                }
                tokio::task::yield_now().await;
                for _ in 1..budget {
                    coop::consume_budget().await;
                }
                assert!(coop::has_budget_remaining());

                let mut read = OwnedRead::new(8_000);
                let eof =
                    poll_fn(|context| accepted.poll_read_owned(context, 8_000, 1_000, &mut read))
                        .await
                        .unwrap();
                assert!(!eof);
                assert!(
                    !coop::has_budget_remaining(),
                    "the read's first poll spends the last unit",
                );
                assert_eq!(
                    read.len(),
                    burst.len(),
                    "continuation polls spend no budget, so a readable burst of at \
                     most maximum bytes still comes back from one read",
                );
                let data = read.into_data().unwrap();
                let received: Vec<u8> = data
                    .segments()
                    .flat_map(|segment| segment.iter().copied())
                    .collect();
                assert!(received == burst);
            });
    }

    #[test]
    fn tcp_read_that_returns_after_a_pending_continuation_releases_its_waker() {
        /// Reads 1_000 queued bytes, so the continuation that follows the
        /// filled first buffer finds the socket empty.
        fn read_queued<S: TokioByteStream>(stream: &mut S, kind: &str) {
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            let polled =
                stream.poll_read_owned(&mut Context::from_waker(&waker), 8_000, 1_000, &mut read);
            assert!(
                matches!(polled, Poll::Ready(Ok(false))),
                "{kind}: {polled:?}"
            );
            assert_eq!(read.len(), 1_000, "{kind}");
            drop(waker);
            assert_eq!(
                Arc::strong_count(&signal),
                1,
                "{kind}: the completed read left no clone of its waker behind",
            );
        }

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let connected = || async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let mut peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    peer.write_all(&pattern(1_000, 7)).await.unwrap();
                    let mut queued = [0; 1_000];
                    while accepted.peek(&mut queued).await.unwrap() < queued.len() {
                        tokio::task::yield_now().await;
                    }
                    (accepted, peer)
                };

                let (mut stream, _peer) = connected().await;
                read_queued(&mut stream, "TcpStream");
                let (stream, _peer) = connected().await;
                read_queued(&mut AcceptedTcpStream::new(stream), "AcceptedTcpStream");
            });
    }

    /// Leaves the current task with no Tokio cooperative budget.
    async fn exhaust_coop_budget() {
        for _ in 0..1_000 {
            if !coop::has_budget_remaining() {
                return;
            }
            coop::consume_budget().await;
        }
        panic!("Tokio's cooperative budget never ran out");
    }

    #[test]
    fn idle_tcp_read_ended_without_data_releases_its_waker() {
        #[derive(Clone, Copy, Debug)]
        enum Stream {
            Tokio,
            Accepted,
        }
        #[derive(Clone, Copy, Debug)]
        enum End {
            Drop,
            Cancel,
            Timeout,
        }

        for stream in [Stream::Tokio, Stream::Accepted] {
            for end in [End::Drop, End::Cancel, End::Timeout] {
                Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(LocalSet::new().run_until(async move {
                        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let peer = TcpStream::connect(listener.local_addr().unwrap())
                            .await
                            .unwrap();
                        let (accepted, _) = listener.accept().await.unwrap();
                        let accepted = accepted.into_std().unwrap();
                        let (worker, driver) = WorkerDriver::new(8).unwrap();
                        let call = worker
                            .call_local(move |context| async move {
                                let transport = match stream {
                                    Stream::Tokio => owned_from_stream(
                                        &context,
                                        TcpStream::from_std(accepted).unwrap(),
                                        SocketAddress::new("127.0.0.1", 1000),
                                        SocketAddress::new("127.0.0.1", 2000),
                                        None,
                                        limits(),
                                    ),
                                    Stream::Accepted => {
                                        TcpTransport::from_accepted(&context, accepted, limits())
                                    }
                                }
                                .unwrap();
                                let cancellation = CancellationSource::new();
                                let timeout = match end {
                                    End::Timeout => Duration::from_millis(10),
                                    End::Drop | End::Cancel => Duration::ZERO,
                                };
                                let signal = Arc::new(WakeSignal::default());
                                let waker = Waker::from(Arc::clone(&signal));
                                let mut read =
                                    transport.read(&context, 64, 7, timeout, cancellation.token());
                                assert!(
                                    read.as_mut()
                                        .poll(&mut Context::from_waker(&waker))
                                        .is_pending()
                                );
                                // A dropped or cancelled read ends in a task with no budget
                                // left; Tokio's timer cannot fire without budget.
                                let expected = match end {
                                    End::Drop => {
                                        exhaust_coop_budget().await;
                                        None
                                    }
                                    End::Cancel => {
                                        assert!(cancellation.request_cancellation());
                                        exhaust_coop_budget().await;
                                        Some(HttpErrorCode::CORO_CANCELLED)
                                    }
                                    End::Timeout => Some(HttpErrorCode::READ_TIMEOUT),
                                };
                                if let Some(expected) = expected {
                                    let result = loop {
                                        if let Poll::Ready(result) =
                                            read.as_mut().poll(&mut Context::from_waker(&waker))
                                        {
                                            break result;
                                        }
                                        assert!(
                                            matches!(end, End::Timeout),
                                            "{stream:?} {end:?}: a cancelled read ends at once",
                                        );
                                        signal.wait().await;
                                    };
                                    assert_eq!(
                                        result.unwrap_err().code(),
                                        expected,
                                        "{stream:?} {end:?}"
                                    );
                                }
                                drop(read);
                                // Runs the wakes Tokio deferred for the exhausted budget.
                                tokio::task::yield_now().await;
                                drop(waker);
                                assert_eq!(
                                    Arc::strong_count(&signal),
                                    1,
                                    "{stream:?} {end:?}: Tokio's reactor kept no clone of the \
                                     idle read's waker",
                                );
                                transport.close(&context).unwrap();
                            })
                            .unwrap();
                        let driver_task = tokio::task::spawn_local(driver);
                        time::timeout(Duration::from_secs(5), call)
                            .await
                            .expect("idle TCP read cleanup remains bounded")
                            .unwrap();
                        worker.begin_shutdown();
                        driver_task.await.unwrap();
                        drop(peer);
                    }));
            }
        }
    }

    /// Leaves the current task exactly one unit of a fresh Tokio budget.
    async fn leave_one_coop_budget_unit() {
        tokio::task::yield_now().await;
        let mut budget = 0;
        while coop::has_budget_remaining() {
            coop::consume_budget().await;
            budget += 1;
        }
        tokio::task::yield_now().await;
        for _ in 1..budget {
            coop::consume_budget().await;
        }
        assert!(coop::has_budget_remaining());
    }

    #[derive(Clone, Copy, Debug)]
    enum TcpReadType {
        Tokio,
        Accepted,
    }

    /// Dispatches to the read of the `TcpStream` or `AcceptedTcpStream` under
    /// test.
    enum TcpReadStream {
        Tokio(TcpStream),
        Accepted(AcceptedTcpStream),
    }

    impl TcpReadStream {
        fn new(kind: TcpReadType, stream: TcpStream) -> Self {
            match kind {
                TcpReadType::Tokio => Self::Tokio(stream),
                TcpReadType::Accepted => Self::Accepted(AcceptedTcpStream::new(stream)),
            }
        }

        fn poll_read_owned(
            &mut self,
            context: &mut Context<'_>,
            maximum: usize,
            allocation_size: usize,
            read: &mut OwnedRead,
        ) -> Poll<io::Result<bool>> {
            match self {
                Self::Tokio(stream) => {
                    stream.poll_read_owned(context, maximum, allocation_size, read)
                }
                Self::Accepted(stream) => {
                    stream.poll_read_owned(context, maximum, allocation_size, read)
                }
            }
        }

        fn poll_with(
            &mut self,
            waker: &Waker,
            maximum: usize,
            allocation_size: usize,
            read: &mut OwnedRead,
        ) -> Poll<io::Result<bool>> {
            self.poll_read_owned(
                &mut Context::from_waker(waker),
                maximum,
                allocation_size,
                read,
            )
        }

        async fn read(&mut self, maximum: usize, allocation_size: usize) -> (OwnedRead, bool) {
            let mut read = OwnedRead::new(maximum);
            let eof = time::timeout(
                Duration::from_secs(5),
                poll_fn(|context| {
                    self.poll_read_owned(context, maximum, allocation_size, &mut read)
                }),
            )
            .await
            .expect("a TCP read of queued bytes completes")
            .unwrap();
            (read, eof)
        }
    }

    /// An accepted socket read through one of the two types, its connected
    /// peer, and a nonblocking clone of the reading socket.
    struct TcpReadCase {
        stream: TcpReadStream,
        peer: TcpStream,
        observer: std::net::TcpStream,
    }

    /// Runs `test` on a fresh runtime for each type of Roxygen-owned TCP read.
    fn on_tcp_read_types<Work>(test: impl Fn(TcpReadCase, TcpReadType) -> Work)
    where
        Work: Future<Output = ()>,
    {
        for kind in [TcpReadType::Tokio, TcpReadType::Accepted] {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    peer.set_nodelay(true).unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let accepted = accepted.into_std().unwrap();
                    let observer = accepted.try_clone().unwrap();
                    let case = TcpReadCase {
                        stream: TcpReadStream::new(kind, TcpStream::from_std(accepted).unwrap()),
                        peer,
                        observer,
                    };
                    test(case, kind).await;
                });
        }
    }

    fn owned_bytes(read: OwnedRead) -> Vec<u8> {
        read.into_data()
            .unwrap()
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect()
    }

    #[test]
    fn idle_tcp_read_holds_no_buffer_until_readable() {
        on_tcp_read_types(|case, kind| async move {
            let TcpReadCase {
                mut stream,
                mut peer,
                observer,
            } = case;
            let reservations = tcp_first_reservations_for_test();
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            assert!(
                stream
                    .poll_with(&waker, 8_000, 1_000, &mut read)
                    .is_pending(),
                "{kind:?}",
            );
            assert_eq!(
                read.held_capacity_for_test(),
                0,
                "{kind:?}: a read waiting on an idle socket holds no buffer",
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations,
                "{kind:?}: the read reserves nothing before the socket is readable",
            );

            let burst = pattern(700, 3);
            peer.write_all(&burst).await.unwrap();
            time::timeout(Duration::from_secs(5), signal.wait())
                .await
                .unwrap_or_else(|_| panic!("{kind:?}: read readiness wakes the caller's waker"));
            wait_until_queued(&observer, burst.len()).await;
            let polled = stream.poll_with(&waker, 8_000, 1_000, &mut read);
            assert!(
                matches!(polled, Poll::Ready(Ok(false))),
                "{kind:?}: {polled:?}"
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations + 1,
                "{kind:?}: the readable socket gets the read's one first-step reservation",
            );
            assert!(owned_bytes(read) == burst, "{kind:?}");

            // Linux reports a reset with no queued bytes once, then EOF.
            #[cfg(target_os = "linux")]
            {
                let mut read = OwnedRead::new(8_000);
                assert!(
                    stream
                        .poll_with(&waker, 8_000, 1_000, &mut read)
                        .is_pending(),
                    "{kind:?}",
                );
                #[expect(deprecated, reason = "Tokio's only linger-zero reset setter")]
                let linger = peer.set_linger(Some(Duration::ZERO));
                linger.unwrap();
                drop(peer);
                time::timeout(Duration::from_secs(5), signal.wait())
                    .await
                    .unwrap_or_else(|_| panic!("{kind:?}: the reset wakes the idle read"));
                let polled = stream.poll_with(&waker, 8_000, 1_000, &mut read);
                assert!(
                    matches!(
                        &polled,
                        Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::ConnectionReset
                    ),
                    "{kind:?}: the read's first step returns the reset: {polled:?}",
                );
                let (read, eof) = stream.read(8_000, 1_000).await;
                assert!(
                    eof && read.is_empty(),
                    "{kind:?}: the next read ends at EOF"
                );
            }
        });

        // Tokio's readiness poll fails only once its I/O driver has shut down.
        for kind in [TcpReadType::Tokio, TcpReadType::Accepted] {
            let runtime = Builder::new_current_thread().enable_all().build().unwrap();
            let (accepted, mut peer) = runtime.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let peer = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                (listener.accept().await.unwrap().0, peer)
            });
            drop(runtime);
            let mut stream = TcpReadStream::new(kind, accepted);
            let reservations = tcp_first_reservations_for_test();
            let mut read = OwnedRead::new(8_000);
            let polled = stream.poll_with(Waker::noop(), 8_000, 1_000, &mut read);
            let mut byte = [0];
            let expected = Pin::new(&mut peer).poll_read(
                &mut Context::from_waker(Waker::noop()),
                &mut ReadBuf::new(&mut byte),
            );
            let (Poll::Ready(Err(error)), Poll::Ready(Err(expected))) = (&polled, &expected) else {
                panic!("{kind:?}: {polled:?}, Tokio's poll_read: {expected:?}");
            };
            assert_eq!(
                (error.kind(), error.to_string()),
                (expected.kind(), expected.to_string()),
                "{kind:?}: the read returns the readiness error Tokio's poll_read returns",
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations,
                "{kind:?}: a failed readiness poll reserves nothing",
            );
        }
    }

    #[test]
    fn tcp_read_after_readiness_keeps_its_lengths_and_capacities() {
        const ALLOCATION_SIZE: usize = 1_000;
        const MAXIMUM: usize = 8_000;
        // (burst, returned segment lengths, returned backing capacity): offers
        // of 1_000, 1_000, 2_000 and 4_000 bytes, ending at a short poll, a
        // pending continuation whose empty buffer is freed, or MAXIMUM.
        let cases: [(usize, &[usize], usize); 9] = [
            (1, &[1], 1_000),
            (999, &[999], 1_000),
            (1_000, &[1_000], 1_000),
            (1_001, &[1_000, 1], 2_000),
            (2_000, &[1_000, 1_000], 2_000),
            (2_001, &[1_000, 1_000, 1], 4_000),
            (4_000, &[1_000, 1_000, 2_000], 4_000),
            (4_001, &[1_000, 1_000, 2_000, 1], 8_000),
            (8_000, &[1_000, 1_000, 2_000, 4_000], 8_000),
        ];
        on_tcp_read_types(|case, kind| async move {
            let TcpReadCase {
                mut stream,
                mut peer,
                observer,
            } = case;
            for (seed, (length, lengths, capacity)) in (0u8..).zip(cases) {
                let burst = pattern(length, seed);
                peer.write_all(&burst).await.unwrap();
                wait_until_queued(&observer, length).await;
                let (read, eof) = stream.read(MAXIMUM, ALLOCATION_SIZE).await;
                assert!(!eof, "{kind:?} {length}");
                let data = read.into_data().unwrap();
                assert_eq!(
                    data.segments()
                        .map(|segment| segment.len())
                        .collect::<Vec<_>>(),
                    lengths,
                    "{kind:?}: a {length}-byte burst",
                );
                assert_eq!(
                    data.backing_capacity_upper_bound(),
                    Some(capacity),
                    "{kind:?}: a {length}-byte burst",
                );
                assert_eq!(
                    data.front().map(|segment| segment.as_ptr() as usize),
                    last_tcp_first_reservation_for_test(),
                    "{kind:?}: a {length}-byte burst's first buffer is the one the read \
                     offered to Tokio, not a staging copy",
                );
                let received: Vec<u8> = data
                    .segments()
                    .flat_map(|segment| segment.iter().copied())
                    .collect();
                assert!(received == burst, "{kind:?} {length}");
            }

            drop(peer);
            let (read, eof) = stream.read(MAXIMUM, ALLOCATION_SIZE).await;
            assert!(eof, "{kind:?}: the peer's FIN ends the next read");
            assert!(read.is_empty(), "{kind:?}");
        });
    }

    #[test]
    fn tcp_read_that_finds_no_data_after_readiness_releases_its_reservation() {
        on_tcp_read_types(|case, kind| async move {
            let TcpReadCase {
                mut stream,
                mut peer,
                mut observer,
            } = case;
            peer.write_all(&pattern(2_000, 4)).await.unwrap();
            wait_until_queued(&observer, 2_000).await;
            let (first, _) = stream.read(1_000, 1_000).await;
            assert_eq!(
                first.len(),
                1_000,
                "{kind:?}: the read stops at its maximum without a short read, so \
                 Tokio keeps the socket's read readiness",
            );
            let mut drained = [0; 1_000];
            observer.read_exact(&mut drained).unwrap();

            let reservations = tcp_first_reservations_for_test();
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            assert!(
                stream
                    .poll_with(&waker, 8_000, 1_000, &mut read)
                    .is_pending(),
                "{kind:?}",
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations + 1,
                "{kind:?}: the cached readiness lets the read reserve and poll the socket",
            );
            assert_eq!(
                read.held_capacity_for_test(),
                0,
                "{kind:?}: a read that finds no data releases its reservation",
            );

            let burst = pattern(300, 5);
            peer.write_all(&burst).await.unwrap();
            time::timeout(Duration::from_secs(5), signal.wait())
                .await
                .unwrap_or_else(|_| panic!("{kind:?}: new data wakes the pending read"));
            wait_until_queued(&observer, burst.len()).await;
            let polled = stream.poll_with(&waker, 8_000, 1_000, &mut read);
            assert!(
                matches!(polled, Poll::Ready(Ok(false))),
                "{kind:?}: {polled:?}"
            );
            assert!(owned_bytes(read) == burst, "{kind:?}");
        });
    }

    /// Tokio clears read readiness after a short read only on its epoll and
    /// kqueue selectors (`PollEvented::poll_read`).
    #[cfg(target_os = "linux")]
    #[test]
    fn idle_tcp_read_after_a_short_read_reserves_nothing_and_spends_no_budget() {
        on_tcp_read_types(|case, kind| async move {
            let TcpReadCase {
                mut stream,
                mut peer,
                observer,
            } = case;
            let burst = pattern(600, 6);
            peer.write_all(&burst).await.unwrap();
            wait_until_queued(&observer, burst.len()).await;
            let (short, eof) = stream.read(8_000, 1_000).await;
            assert!(!eof, "{kind:?}");
            assert_eq!(
                short.len(),
                600,
                "{kind:?}: 600 bytes do not fill the 1_000-byte offer",
            );

            leave_one_coop_budget_unit().await;
            let reservations = tcp_first_reservations_for_test();
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            assert!(
                stream
                    .poll_with(&waker, 8_000, 1_000, &mut read)
                    .is_pending(),
                "{kind:?}",
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations,
                "{kind:?}: Tokio cleared its read readiness after the short read, so \
                 the idle read reserves nothing",
            );
            assert_eq!(read.held_capacity_for_test(), 0, "{kind:?}");
            assert!(
                coop::has_budget_remaining(),
                "{kind:?}: the idle read leaves the task's last budget unit",
            );

            let next = pattern(200, 7);
            peer.write_all(&next).await.unwrap();
            time::timeout(Duration::from_secs(5), signal.wait())
                .await
                .unwrap_or_else(|_| panic!("{kind:?}: new data wakes the idle read"));
            wait_until_queued(&observer, next.len()).await;
            let polled = stream.poll_with(&waker, 8_000, 1_000, &mut read);
            assert!(
                matches!(polled, Poll::Ready(Ok(false))),
                "{kind:?}: {polled:?}"
            );
            assert!(owned_bytes(read) == next, "{kind:?}");
        });
    }

    #[test]
    fn tcp_read_without_budget_yields_before_reserving() {
        on_tcp_read_types(|case, kind| async move {
            let TcpReadCase {
                mut stream,
                mut peer,
                observer,
            } = case;
            exhaust_coop_budget().await;
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            assert!(
                stream
                    .poll_with(&waker, 8_000, 1_000, &mut read)
                    .is_pending(),
                "{kind:?}",
            );
            tokio::task::yield_now().await;
            assert!(
                signal.take(),
                "{kind:?}: Tokio wakes a read refused for budget on an idle socket once the \
                 task yields",
            );
            drop(waker);
            assert_eq!(
                Arc::strong_count(&signal),
                1,
                "{kind:?}: the refused idle read left no waker in Tokio's reactor",
            );

            let queued = pattern(6_000, 8);
            peer.write_all(&queued).await.unwrap();
            wait_until_queued(&observer, queued.len()).await;
            let (first, _) = stream.read(1_000, 1_000).await;
            assert_eq!(
                first.len(),
                1_000,
                "{kind:?}: the read stops at its maximum without a short read, so \
                 Tokio keeps the socket's read readiness",
            );
            let burst = queued[1_000..].to_vec();

            exhaust_coop_budget().await;
            let reservations = tcp_first_reservations_for_test();
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut read = OwnedRead::new(8_000);
            assert!(
                stream
                    .poll_with(&waker, 8_000, 1_000, &mut read)
                    .is_pending(),
                "{kind:?}",
            );
            assert_eq!(
                tcp_first_reservations_for_test(),
                reservations,
                "{kind:?}: a read refused for budget reserves nothing",
            );
            assert_eq!(read.held_capacity_for_test(), 0, "{kind:?}");
            assert!(read.is_empty(), "{kind:?}");
            let mut queued = vec![0; burst.len()];
            assert_eq!(
                observer.peek(&mut queued).unwrap(),
                burst.len(),
                "{kind:?}: the refused read reads no byte",
            );
            tokio::task::yield_now().await;
            assert!(
                signal.take(),
                "{kind:?}: Tokio wakes the refused read once the task yields",
            );

            leave_one_coop_budget_unit().await;
            let polled = stream.poll_with(&waker, 8_000, 1_000, &mut read);
            assert!(
                matches!(polled, Poll::Ready(Ok(false))),
                "{kind:?}: {polled:?}"
            );
            assert!(
                !coop::has_budget_remaining(),
                "{kind:?}: the read's first step spends the last budget unit",
            );
            assert_eq!(
                read.len(),
                burst.len(),
                "{kind:?}: continuation polls spend no budget, so the whole burst \
                 above allocation_size comes back",
            );
            assert!(owned_bytes(read) == burst, "{kind:?}");
        });
    }

    /// Pins the socket behavior `DIV-016` relies on: Linux returns the bytes
    /// queued before a reset, then reports the reset once, then EOF.
    #[cfg(target_os = "linux")]
    #[test]
    fn tcp_reset_after_a_filled_buffer_fails_the_next_read_once() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let peer = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (accepted, _) = listener.accept().await.unwrap();
                let accepted = accepted.into_std().unwrap();
                let observer = accepted.try_clone().unwrap();
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::from_accepted(
                            &context,
                            accepted,
                            ByteStreamLimits::new(8_000, 1024, 8).unwrap(),
                        )
                        .unwrap();
                        let burst = pattern(1_000, 9);
                        let mut peer = peer;
                        peer.write_all(&burst).await.unwrap();
                        #[expect(deprecated, reason = "Tokio's only linger-zero reset setter")]
                        let linger = peer.set_linger(Some(Duration::ZERO));
                        linger.unwrap();
                        drop(peer);
                        TcpStream::from_std(observer)
                            .unwrap()
                            .ready(Interest::ERROR)
                            .await
                            .unwrap();
                        let read = || {
                            transport.read(
                                &context,
                                8_000,
                                1_000,
                                Duration::from_secs(5),
                                CancellationToken::never(),
                            )
                        };

                        let first = read().await.unwrap();
                        assert!(!first.is_eof());
                        assert!(
                            read_bytes(first) == burst,
                            "the bytes queued before the reset come first, although the \
                             poll after them failed",
                        );
                        let error = read().await.unwrap_err();
                        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                        let cause = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
                        assert_eq!(cause.kind(), io::ErrorKind::ConnectionReset);
                        assert!(
                            read().await.unwrap().is_eof(),
                            "the socket reports the reset only once",
                        );
                        transport.close_with_reset(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("reads after a TCP reset remain bounded")
                    .unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    /// More bytes than the peer's 4096-byte receive buffer holds, which the
    /// accepted stream's large send buffer takes in one write.
    #[cfg(target_os = "linux")]
    const UNRECEIVED_BURST: usize = 48 * 1024;

    /// Writes `UNRECEIVED_BURST` bytes on an accepted stream whose peer reads
    /// nothing until `close_with_reset` has closed it; returns how many bytes the
    /// peer then read and whether the connection ended with a reset.
    #[cfg(target_os = "linux")]
    fn reset_with_bytes_in_flight(non_abortive: bool) -> (usize, bool) {
        let _spawn = crate::TEST_PROCESS_SPAWN_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpSocket::new_v4().unwrap();
                // Accepted sockets inherit the listener's send buffer.
                listener.set_send_buffer_size(256 * 1024).unwrap();
                listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let listener = listener.listen(8).unwrap();
                let peer = TcpSocket::new_v4().unwrap();
                peer.set_recv_buffer_size(4096).unwrap();
                let mut peer = peer.connect(listener.local_addr().unwrap()).await.unwrap();
                let (accepted, _) = listener.accept().await.unwrap();
                let accepted = accepted.into_std().unwrap();
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::from_accepted(
                            &context,
                            accepted,
                            ByteStreamLimits::new(8_000, UNRECEIVED_BURST, 8).unwrap(),
                        )
                        .unwrap();
                        if non_abortive {
                            transport.enable_non_abortive_close(&context).unwrap();
                        }
                        transport
                            .write(
                                &context,
                                BufQueue::from_vec(vec![b'x'; UNRECEIVED_BURST], UNRECEIVED_BURST)
                                    .unwrap(),
                                Duration::from_secs(5),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        transport.close_with_reset(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("the write and the reset finish")
                    .unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
                let mut received = 0;
                loop {
                    let mut chunk = [0; 4096];
                    let read = time::timeout(Duration::from_secs(5), peer.read(&mut chunk))
                        .await
                        .expect("the connection ends");
                    match read {
                        Ok(0) => return (received, false),
                        Ok(read) => received += read,
                        Err(error) => {
                            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
                            return (received, true);
                        }
                    }
                }
            }))
    }

    /// Pins the default: the linger-zero close discards the bytes still in the
    /// send buffer and resets the peer.
    #[cfg(target_os = "linux")]
    #[test]
    fn tcp_reset_discards_bytes_the_peer_has_not_received() {
        let (received, reset) = reset_with_bytes_in_flight(false);
        assert!(reset, "the abortive close resets the peer");
        assert!(
            received < UNRECEIVED_BURST,
            "the bytes left in the send buffer are discarded: {received} arrived"
        );
    }

    /// With `enable_non_abortive_close`, a reset closes the socket as dropping a
    /// Tokio stream does: the kernel still sends every written byte, then FIN.
    #[cfg(target_os = "linux")]
    #[test]
    fn tcp_reset_with_non_abortive_close_sends_written_bytes_then_fin() {
        assert_eq!(
            reset_with_bytes_in_flight(true),
            (UNRECEIVED_BURST, false),
            "every written byte arrives, then EOF"
        );
    }

    /// Reads until EOF, checking each read's bound, and returns the bytes and
    /// the size of every read.
    async fn read_to_eof(
        context: &roxygen_runtime::worker::WorkerContext,
        transport: &dyn ByteStreamTransport,
        maximum: usize,
        allocation_size: usize,
    ) -> (Vec<u8>, Vec<usize>) {
        let mut received = Vec::new();
        let mut sizes = Vec::new();
        loop {
            let read = transport
                .read(
                    context,
                    maximum,
                    allocation_size,
                    Duration::from_secs(5),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            if read.is_eof() {
                assert_eq!(read.bytes_read(), 0, "EOF follows the last bytes");
                return (received, sizes);
            }
            assert!((1..=maximum).contains(&read.bytes_read()));
            sizes.push(read.bytes_read());
            received.extend(read_bytes(read));
        }
    }

    #[test]
    fn allocation_sized_reads_preserve_bytes_and_eof() {
        // With a 7-byte allocation size and a 64-byte maximum, a 7-byte payload
        // fills the first buffer and its EOF ends the next poll; 64 bytes fill a
        // whole read; 1_000 bytes take many reads.
        for (length, expected_sizes) in [
            (7, vec![7]),
            (64, vec![64]),
            (1_000, [vec![64; 15], vec![40]].concat()),
        ] {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let mut peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let payload = pattern(length, 3);
                    peer.write_all(&payload).await.unwrap();
                    peer.shutdown().await.unwrap();
                    let mut queued = vec![0; length];
                    while accepted.peek(&mut queued).await.unwrap() < length {
                        tokio::task::yield_now().await;
                    }
                    let accepted = accepted.into_std().unwrap();
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport =
                                TcpTransport::from_accepted(&context, accepted, limits()).unwrap();
                            let (received, sizes) = read_to_eof(&context, &transport, 64, 7).await;
                            assert!(received == payload, "{length}-byte payload");
                            assert_eq!(sizes, expected_sizes, "{length}-byte payload");
                            transport.close(&context).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("TCP reads to EOF remain bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                    drop(peer);
                }));
        }
    }

    #[test]
    fn tls_allocation_sized_reads_preserve_bytes_and_eof() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (certificate, server_config) = tls_fixture();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let acceptor = TlsAcceptor::from(server_config);
                let payload = pattern(5_000, 5);
                let sent = payload.clone();
                let server = tokio::task::spawn_local(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = acceptor.accept(stream).await.unwrap();
                    for chunk in sent.chunks(1_500) {
                        stream.write_all(chunk).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                    stream.shutdown().await.unwrap();
                    let mut eof = [0; 1];
                    let _ = stream.read(&mut eof).await;
                });
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let mut options = TlsClientOptions::new("localhost");
                        options.add_root_certificate(certificate);
                        let transport = TlsTransport::connect(
                            &context,
                            address(destination),
                            options,
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        let (received, _) = read_to_eof(&context, &transport, 64, 7).await;
                        assert!(received == payload);
                        transport.close(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("TLS reads to EOF remain bounded")
                    .unwrap();
                worker.begin_shutdown();
                time::timeout(Duration::from_secs(5), driver_task)
                    .await
                    .expect("worker shutdown remains bounded")
                    .unwrap();
                time::timeout(Duration::from_secs(5), server)
                    .await
                    .expect("TLS peer remains bounded")
                    .unwrap();
            }));
    }

    #[test]
    fn owned_chain_reaches_tcp_writer() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(|context| async move {
                        let write_pointers = Rc::new(RefCell::new(Vec::new()));
                        let stream = BackingProbeStream {
                            read_payload: Box::default(),
                            read_offset: 0,
                            read_allocations: Rc::new(RefCell::new(Vec::new())),
                            write_pointers: Rc::clone(&write_pointers),
                        };
                        let transport = owned_from_stream(
                            &context,
                            stream,
                            SocketAddress::new("127.0.0.1", 1000),
                            SocketAddress::new("127.0.0.1", 2000),
                            None,
                            limits(),
                        )
                        .expect("probe transport is adopted");
                        let first = vec![b'a'; 8];
                        let second = vec![b'b'; 5];
                        let expected = vec![first.as_ptr() as usize, second.as_ptr() as usize];
                        let mut data = BufQueue::new(first.len() + second.len());
                        data.push_bytes(first.into())
                            .expect("first owned segment fits the write bound");
                        data.push_bytes(second.into())
                            .expect("second owned segment fits the write bound");

                        let written = transport
                            .write(&context, data, Duration::ZERO, CancellationToken::never())
                            .await
                            .expect("probe write succeeds");
                        assert_eq!(written.bytes_written, 13);
                        assert_eq!(*write_pointers.borrow(), vec![expected]);

                        transport.close(&context).expect("transport closes");
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn tcp_loopback_preserves_byte_stream_contract() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let server = tokio::task::spawn_local(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = [0; 6];
                    stream.read_exact(&mut request).await.unwrap();
                    assert_eq!(&request, b"abcdef");
                    stream.write_all(b"reply").await.unwrap();
                    let mut eof = [0; 1];
                    assert_eq!(stream.read(&mut eof).await.unwrap(), 0);
                    stream.write_all(b"tail").await.unwrap();
                    stream.shutdown().await.unwrap();

                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut byte = [0; 1];
                    stream.read_exact(&mut byte).await.unwrap();
                    assert_eq!(byte, *b"x");
                    let error = stream
                        .read(&mut byte)
                        .await
                        .expect_err("reset close must be abortive");
                    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                });

                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::connect(
                            &context,
                            address(destination),
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        transport.enable_zero_copy(&context, 32_768).unwrap();
                        assert_eq!(
                            transport.byte_event_capability(&context).unwrap(),
                            ByteEventCapability::Unsupported,
                        );
                        let unsupported_events = Rc::new(TransportEventLog::default());
                        assert!(
                            transport
                                .register_byte_event(
                                    &context,
                                    HttpByteEventRegistration::new(
                                        None,
                                        ByteEventType::NicTx.bit()
                                            | ByteEventType::CumulativeAck.bit(),
                                        &unsupported_events,
                                    ),
                                )
                                .is_err()
                        );
                        assert!(unsupported_events.fired.borrow().is_empty());
                        assert_eq!(
                            unsupported_events
                                .cancelled
                                .borrow()
                                .iter()
                                .map(|(event, code)| (event.event_type, *code))
                                .collect::<Vec<_>>(),
                            vec![
                                (
                                    ByteEventType::NicTx,
                                    HttpErrorCode::INVALID_STATE_TRANSITION,
                                ),
                                (
                                    ByteEventType::CumulativeAck,
                                    HttpErrorCode::INVALID_STATE_TRANSITION,
                                ),
                            ],
                        );
                        assert_eq!(
                            transport.peer_address(&context).unwrap().port(),
                            destination.port(),
                        );
                        assert!(transport.local_address(&context).unwrap().port() != 0);
                        assert_eq!(transport.peer_identity(&context).unwrap(), None);

                        let written = transport
                            .write(
                                &context,
                                queue(&[b"ab", b"cd", b"ef"]),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(written.bytes_written, 6);

                        let mut reply = Vec::new();
                        while reply.len() < 5 {
                            let read = transport
                                .read(
                                    &context,
                                    3,
                                    2,
                                    Duration::from_secs(1),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            assert!(!read.is_eof());
                            assert!(read.bytes_read() <= 3);
                            assert!(read.data().segments().all(|segment| segment.len() <= 2));
                            reply.extend(read_bytes(read));
                        }
                        assert_eq!(reply, b"reply");

                        time::pause();
                        let mut timeout_read = transport.read(
                            &context,
                            1,
                            1,
                            Duration::from_millis(5),
                            CancellationToken::never(),
                        );
                        assert!(poll_once(timeout_read.as_mut()).is_pending());
                        time::advance(Duration::from_millis(5)).await;
                        assert_eq!(
                            timeout_read.await.unwrap_err().code(),
                            HttpErrorCode::READ_TIMEOUT,
                        );
                        time::resume();

                        transport.shutdown_write(&context).unwrap();
                        let mut tail = Vec::new();
                        loop {
                            let read = transport
                                .read(
                                    &context,
                                    4,
                                    2,
                                    Duration::from_secs(1),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            if read.is_eof() {
                                break;
                            }
                            tail.extend(read_bytes(read));
                        }
                        assert_eq!(tail, b"tail");

                        let cancellation = CancellationSource::new();
                        cancellation.request_cancellation();
                        let error = match TcpTransport::connect(
                            &context,
                            address(destination),
                            limits(),
                            Duration::from_secs(1),
                            cancellation.token(),
                        )
                        .await
                        {
                            Ok(_) => panic!("pre-cancelled TCP connect must fail"),
                            Err(error) => error,
                        };
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                        let error = match TcpTransport::connect(
                            &context,
                            SocketAddress::default(),
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        {
                            Ok(_) => panic!("uninitialized TCP address must fail"),
                            Err(error) => error,
                        };
                        assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR);

                        let reset = TcpTransport::connect(
                            &context,
                            address(destination),
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        reset
                            .write(
                                &context,
                                queue(&[b"x"]),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        reset.close_with_reset(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("TCP client I/O must remain bounded")
                    .unwrap();
                worker.begin_shutdown();
                time::timeout(Duration::from_secs(5), driver_task)
                    .await
                    .expect("worker shutdown must remain bounded")
                    .unwrap();
                time::timeout(Duration::from_secs(5), server)
                    .await
                    .expect("TCP server I/O must remain bounded")
                    .unwrap();
            }));

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let server = tokio::task::spawn_local(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut byte = [0; 1];
                    let result = stream.read(&mut byte).await;
                    assert!(matches!(result, Ok(0) | Err(_)));
                });
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::connect(
                            &context,
                            address(destination),
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        let mut read = transport.read(
                            &context,
                            1,
                            1,
                            Duration::ZERO,
                            CancellationToken::never(),
                        );
                        assert!(poll_once(read.as_mut()).is_pending());
                        ESCAPED_TCP.with(|escaped| {
                            assert!(escaped.replace(Some((transport, read))).is_none());
                        });
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("escaped TCP setup must remain bounded")
                    .unwrap();
                assert!(worker.begin_shutdown());
                time::timeout(Duration::from_secs(5), driver_task)
                    .await
                    .expect("worker shutdown must remain bounded")
                    .unwrap();
                assert_eq!(worker.state(), WorkerState::Closed);
                time::timeout(Duration::from_secs(5), server)
                    .await
                    .expect("escaped TCP peer read must remain bounded")
                    .unwrap();
                ESCAPED_TCP.with(|escaped| drop(escaped.replace(None)));
            }));
    }

    #[test]
    fn escaped_connect_and_handshake_setup_drain_while_closing() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let owner = std::thread::current().id();
                    let drops = Arc::new(AtomicUsize::new(0));
                    let states = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let threads = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let probe_worker = worker.clone();
                    let probe_drops = Arc::clone(&drops);
                    let probe_states = Arc::clone(&states);
                    let probe_threads = Arc::clone(&threads);
                    let call = worker
                        .call_local(move |context| async move {
                            let probe = SetupDropProbe {
                                drops: probe_drops,
                                states: probe_states,
                                threads: probe_threads,
                                worker: probe_worker,
                            };
                            let setup = setup_future(&context, async move {
                                let _probe = probe;
                                std::future::pending::<Result<(), HttpError>>().await
                            })
                            .unwrap();
                            ESCAPED_SETUP.with(|escaped| {
                                assert!(escaped.replace(Some(setup)).is_none());
                            });
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("pending setup installation must remain bounded")
                        .unwrap();
                    assert!(worker.begin_shutdown());
                    time::timeout(Duration::from_secs(5), driver_task)
                        .await
                        .expect("setup cleanup must remain bounded")
                        .unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    assert_eq!(drops.load(Ordering::Acquire), 1);
                    assert_eq!(states.lock().unwrap().as_slice(), &[WorkerState::Closing]);
                    assert_eq!(threads.lock().unwrap().as_slice(), &[owner]);
                    ESCAPED_SETUP.with(|escaped| drop(escaped.replace(None)));
                    assert_eq!(drops.load(Ordering::Acquire), 1);
                })
                .await
                .expect("pending connect cleanup must remain bounded");
            }));

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let destination = listener.local_addr().unwrap();
                    let (hello_tx, hello_rx) = tokio::sync::oneshot::channel();
                    let server = tokio::task::spawn_local(async move {
                        let (mut stream, _) = listener.accept().await.unwrap();
                        let mut hello = [0; 1024];
                        assert!(stream.read(&mut hello).await.unwrap() != 0);
                        hello_tx.send(()).unwrap();
                        let mut remaining = Vec::new();
                        stream.read_to_end(&mut remaining).await.unwrap();
                    });
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let mut handshake = TlsTransport::connect(
                                &context,
                                address(destination),
                                TlsClientOptions::new("localhost"),
                                limits(),
                                Duration::ZERO,
                                CancellationToken::never(),
                            );
                            tokio::pin!(hello_rx);
                            tokio::select! {
                                biased;
                                result = &mut handshake => {
                                    panic!("silent peer completed TLS setup: {:?}", result.err());
                                }
                                result = &mut hello_rx => result.unwrap(),
                            }
                            ESCAPED_HANDSHAKE.with(|escaped| {
                                assert!(escaped.replace(Some(handshake)).is_none());
                            });
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("pending TLS handshake installation must remain bounded")
                        .unwrap();
                    assert!(worker.begin_shutdown());
                    time::timeout(Duration::from_secs(5), driver_task)
                        .await
                        .expect("TLS setup cleanup must remain bounded")
                        .unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    time::timeout(Duration::from_secs(5), server)
                        .await
                        .expect("TLS setup peer EOF must remain bounded")
                        .unwrap();
                    ESCAPED_HANDSHAKE.with(|escaped| drop(escaped.replace(None)));
                })
                .await
                .expect("pending TLS handshake cleanup must remain bounded");
            }));
    }

    fn tls_fixture() -> (Vec<u8>, Arc<ServerConfig>) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = generated.cert.der().to_vec();
        let certificate_der = CertificateDer::from(certificate.clone());
        let private_key =
            PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec()).unwrap();
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![certificate_der], private_key)
                .unwrap();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        (certificate, Arc::new(config))
    }

    fn tls_owned_read_payload() -> Vec<u8> {
        (0..=250).cycle().take(48 * 1024 + 37).collect()
    }

    async fn send_tls_owned_read_fixture(
        mut tls: tokio_rustls::TlsStream<TcpStream>,
        more: tokio::sync::oneshot::Receiver<()>,
        read_done: tokio::sync::oneshot::Receiver<()>,
        close_notify: bool,
    ) {
        tls.write_all(b"tiny").await.unwrap();
        tls.flush().await.unwrap();
        more.await.unwrap();
        tls.write_all(&tls_owned_read_payload()).await.unwrap();
        tls.flush().await.unwrap();
        if close_notify {
            tls.shutdown().await.unwrap();
        } else {
            tls.get_mut().0.shutdown().await.unwrap();
        }
        read_done.await.unwrap();
    }

    async fn read_owned_tls_chunk<T: TokioByteStream>(
        stream: &mut T,
        maximum: usize,
    ) -> io::Result<BufQueue> {
        // TLS ignores the allocation size and copies the available plaintext.
        let mut read = OwnedRead::new(maximum);
        let result =
            std::future::poll_fn(|context| stream.poll_read_owned(context, maximum, 1, &mut read))
                .await;
        let amount = read.len();
        let held = read.held_capacity_for_test();
        let allocation = read.tail_for_test().as_ptr();
        let data = read.into_data().unwrap();
        match result {
            Ok(eof) => {
                assert!(amount <= maximum);
                assert_eq!(eof, amount == 0);
                assert_eq!(held, amount, "TLS makes one exact-size copy");
                assert_eq!(data.chain_length(), amount);
                assert_eq!(data.backing_capacity_upper_bound(), Some(amount));
                if let Some(segment) = data.front() {
                    assert_eq!(segment.as_ptr(), allocation);
                }
                Ok(data)
            }
            Err(error) => {
                assert_eq!(amount, 0);
                assert_eq!(held, 0, "a failed TLS read holds no capacity");
                Err(error)
            }
        }
    }

    async fn assert_native_tls_owned_reads<T: TlsStreamAccess>(
        tls: T,
        more: tokio::sync::oneshot::Sender<()>,
        read_done: tokio::sync::oneshot::Sender<()>,
        close_notify: bool,
    ) {
        let mut stream = OwnedTlsStream::new(tls);
        let tiny = read_owned_tls_chunk(&mut stream, 256 * 1024).await.unwrap();
        assert_eq!(tiny.front().unwrap().as_ref(), b"tiny");
        more.send(()).unwrap();

        let expected = tls_owned_read_payload();
        let mut received = 0;
        let mut chunks = Vec::new();
        while received < expected.len() {
            let maximum = [1, 7, 1023, 256 * 1024][chunks.len() % 4];
            let chunk = read_owned_tls_chunk(&mut stream, maximum).await.unwrap();
            assert!(!chunk.is_empty());
            received += chunk.chain_length();
            chunks.push(chunk);
        }
        let eof = read_owned_tls_chunk(&mut stream, 256 * 1024).await;
        if close_notify {
            assert!(eof.unwrap().is_empty());
        } else {
            assert_eq!(eof.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        }
        drop(stream);

        assert_eq!(tiny.front().unwrap().as_ref(), b"tiny");
        assert_eq!(
            chunks
                .iter()
                .flat_map(BufQueue::segments)
                .flat_map(|segment| segment.iter().copied())
                .collect::<Vec<_>>(),
            expected,
        );
        read_done.send(()).unwrap();
    }

    async fn drive_tls_owned_read_handshake<T>(
        handshake: impl Future<Output = io::Result<T>>,
        background: CiphertextBackgroundForTest<TcpStream>,
    ) -> T {
        tokio::pin!(handshake);
        std::future::poll_fn(|context| {
            let result = handshake.as_mut().poll(context);
            let _ = background.poll(context);
            result
        })
        .await
        .unwrap()
    }

    async fn native_tls_owned_read_fixture(server_side: bool, close_notify: bool) {
        let (certificate, server_config) = tls_fixture();
        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(certificate)).unwrap();
        let client_config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));
        let acceptor = TlsAcceptor::from(server_config);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let destination = listener.local_addr().unwrap();
        let (more_tx, more_rx) = tokio::sync::oneshot::channel();
        let (read_done_tx, read_done_rx) = tokio::sync::oneshot::channel();

        let peer = async {
            if server_side {
                let tcp = TcpStream::connect(destination).await.unwrap();
                let tls = connector
                    .connect(ServerName::try_from("localhost").unwrap(), tcp)
                    .await
                    .unwrap();
                send_tls_owned_read_fixture(tls.into(), more_rx, read_done_rx, close_notify).await;
            } else {
                let (tcp, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(tcp).await.unwrap();
                send_tls_owned_read_fixture(tls.into(), more_rx, read_done_rx, close_notify).await;
            }
        };
        let reader = async {
            if server_side {
                let (tcp, _) = listener.accept().await.unwrap();
                let wire = OwnedCiphertextStream::new(tcp);
                let background = wire.background_for_test();
                let tls = drive_tls_owned_read_handshake(acceptor.accept(wire), background).await;
                assert_native_tls_owned_reads(tls, more_tx, read_done_tx, close_notify).await;
            } else {
                let tcp = TcpStream::connect(destination).await.unwrap();
                let wire = OwnedCiphertextStream::new(tcp);
                let background = wire.background_for_test();
                let tls = drive_tls_owned_read_handshake(
                    connector.connect(ServerName::try_from("localhost").unwrap(), wire),
                    background,
                )
                .await;
                assert_native_tls_owned_reads(tls, more_tx, read_done_tx, close_notify).await;
            }
        };
        tokio::join!(reader, peer);
    }

    fn native_tls_owned_read_cases(server_side: bool) {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                for close_notify in [false, true] {
                    time::timeout(
                        Duration::from_secs(5),
                        native_tls_owned_read_fixture(server_side, close_notify),
                    )
                    .await
                    .expect("native TLS owned reads remain bounded");
                }
            });
    }

    #[test]
    fn native_tls_client_owned_reads_allocate_only_available_plaintext() {
        native_tls_owned_read_cases(false);
    }

    #[test]
    fn native_tls_server_owned_reads_allocate_only_available_plaintext() {
        native_tls_owned_read_cases(true);
    }

    mod h1 {
        use super::*;

        #[test]
        fn httpdownstream_session_test_invalid_peer_certificate() {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let (certificate, server_config) = tls_fixture();
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let destination = listener.local_addr().unwrap();
                    let acceptor = TlsAcceptor::from(server_config);
                    let server = tokio::task::spawn_local(async move {
                        let (stream, _) = listener.accept().await.unwrap();
                        let _trusted = acceptor.accept(stream).await.unwrap();
                        let (stream, _) = listener.accept().await.unwrap();
                        assert!(acceptor.accept(stream).await.is_err());
                    });
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            struct BadActorIdentity;
                            impl TlsPeerIdentityVerifier for BadActorIdentity {
                                fn verify(
                                    &self,
                                    _certificate_der: &[u8],
                                ) -> Result<PeerIdentity, HttpError>
                                {
                                    Ok(PeerIdentity::new(b"bad_actor"))
                                }
                            }

                            let mut trusted = TlsClientOptions::new("localhost");
                            trusted.add_root_certificate(&certificate);
                            trusted.set_alpn_protocols([b"http/1.1".as_slice()]);
                            trusted.set_identity_verifier(Rc::new(BadActorIdentity));
                            let transport = TlsTransport::connect(
                                &context,
                                address(destination),
                                trusted,
                                limits(),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                            let owner = transport
                                .zero_copy_owner_for_test(&context)
                                .unwrap()
                                .expect("TLS delegates to one TCP completion owner");
                            let _ = transport.enable_zero_copy(&context, 32_768).unwrap();
                            assert_eq!(
                                transport.zero_copy_owner_for_test(&context).unwrap(),
                                Some(owner),
                            );
                            let peer_identity = transport.peer_identity(&context).unwrap().unwrap();
                            assert_eq!(peer_identity.as_bytes(), b"bad_actor");
                            let response_status = if peer_identity.as_bytes() == b"good_actor" {
                                200
                            } else {
                                407
                            };
                            assert_eq!(response_status, 407);
                            assert_eq!(
                                transport.connection_info().application_protocol(),
                                Some(b"http/1.1".as_slice()),
                            );
                            assert!(transport.connection_info().cipher_suite().is_some());
                            assert_eq!(
                                transport.connection_info().key_exchange_group(),
                                Some("X25519"),
                            );
                            assert!(transport.connection_info().protocol_version().is_some());
                            transport.close(&context).unwrap();

                            let untrusted = TlsClientOptions::new("localhost");
                            let error = match TlsTransport::connect(
                                &context,
                                address(destination),
                                untrusted,
                                limits(),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            {
                                Ok(_) => panic!("untrusted peer certificate must fail"),
                                Err(error) => error,
                            };
                            assert_eq!(error.code(), HttpErrorCode::INADEQUATE_SECURITY);
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("TLS identity policy must remain bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    time::timeout(Duration::from_secs(5), driver_task)
                        .await
                        .expect("worker shutdown must remain bounded")
                        .unwrap();
                    time::timeout(Duration::from_secs(5), server)
                        .await
                        .expect("TLS accept must remain bounded")
                        .unwrap();
                }));
        }
    }

    #[test]
    fn rustls_adapter_does_not_own_session() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (certificate, server_config) = tls_fixture();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let acceptor = TlsAcceptor::from(server_config);
                let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
                let server = tokio::task::spawn_local(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = acceptor.accept(stream).await.unwrap();
                    let mut request = [0; 4];
                    stream.read_exact(&mut request).await.unwrap();
                    assert_eq!(&request, b"ping");
                    stream.write_all(b"pong").await.unwrap();
                    let mut eof = [0; 1];
                    assert_eq!(stream.read(&mut eof).await.unwrap(), 0);
                    closed_tx.send(()).unwrap();
                });
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let session_destroyed = Rc::new(std::cell::Cell::new(false));
                struct SessionOwner(Rc<std::cell::Cell<bool>>);
                impl Drop for SessionOwner {
                    fn drop(&mut self) {
                        self.0.set(true);
                    }
                }
                let owner = SessionOwner(Rc::clone(&session_destroyed));
                let call = worker
                    .call_local(move |context| async move {
                        let mut options = TlsClientOptions::new("localhost");
                        options.add_root_certificate(certificate);
                        let transport = TlsTransport::connect(
                            &context,
                            address(destination),
                            options,
                            limits(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            transport.byte_event_capability(&context).unwrap(),
                            ByteEventCapability::Unsupported,
                        );
                        assert_eq!(
                            transport
                                .write(
                                    &context,
                                    queue(&[b"p", b"ing"]),
                                    Duration::from_secs(1),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap()
                                .bytes_written,
                            4,
                        );
                        let response = transport
                            .read(
                                &context,
                                4,
                                2,
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(read_bytes(response), b"pong");
                        transport.close(&context).unwrap();
                        time::timeout(Duration::from_secs(5), closed_rx)
                            .await
                            .expect("TLS graceful close must drain close_notify")
                            .unwrap();
                        drop(transport);
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("TLS adapter I/O must remain bounded")
                    .unwrap();
                assert!(!session_destroyed.get());
                drop(owner);
                assert!(session_destroyed.get());
                worker.begin_shutdown();
                time::timeout(Duration::from_secs(5), driver_task)
                    .await
                    .expect("worker shutdown must remain bounded")
                    .unwrap();
                time::timeout(Duration::from_secs(5), server)
                    .await
                    .expect("TLS I/O must remain bounded")
                    .unwrap();
            }));
    }

    #[test]
    fn tls_plaintext_fragments_copy_the_selected_byte_range() {
        let chunks: &[&[u8]] = &[b"", b"ab", b"", b"cdefg", b"h", b"", b"ijkl", b""];
        let flat = b"abcdefghijkl";
        for start in 0..=flat.len() {
            for end in start..=flat.len() {
                let payload = OutboundChunks::Multiple { chunks, start, end };
                let mut actual = b"prefix:".to_vec();
                payload.copy_to_vec(&mut actual);
                let mut expected = b"prefix:".to_vec();
                expected.extend_from_slice(&flat[start..end]);
                assert_eq!(actual, expected, "range {start}..{end}");
            }
        }

        let (_, tail) = OutboundChunks::new(chunks).split_at(3);
        let (fragment, _) = tail.split_at(6);
        let mut actual = b"prefix:".to_vec();
        fragment.copy_to_vec(&mut actual);
        assert_eq!(actual, b"prefix:defghi");
    }

    #[test]
    fn tls_accept_preserves_one_tcp_owner_through_reenable_and_io() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let generated =
                    rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
                let certificate = generated.cert.der().to_vec();
                let private_key = generated.signing_key.serialized_der().to_vec();
                let server_options = TlsServerOptions::new(
                    vec![certificate.clone().into_boxed_slice()],
                    private_key.into_boxed_slice(),
                    vec![b"http/1.1".to_vec().into_boxed_slice()],
                )
                .unwrap();
                let mut roots = RootCertStore::empty();
                roots
                    .add(CertificateDer::from(certificate))
                    .expect("generated certificate is a valid root");
                let mut client_config = ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
                client_config.alpn_protocols = vec![b"http/1.1".to_vec()];

                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let client = tokio::task::spawn_local(async move {
                    let stream = TcpStream::connect(destination).await.unwrap();
                    let server_name = ServerName::try_from("localhost").unwrap();
                    let mut tls = TlsConnector::from(Arc::new(client_config))
                        .connect(server_name, stream)
                        .await
                        .unwrap();
                    tls.write_all(b"ping").await.unwrap();
                    let mut response = vec![0; 128 * 1024];
                    tls.read_exact(&mut response).await.unwrap();
                    assert!(response[..64 * 1024].iter().all(|byte| *byte == b'a'));
                    assert!(response[64 * 1024..].iter().all(|byte| *byte == b'b'));
                    tls.shutdown().await.unwrap();
                });
                let (stream, _) = listener.accept().await.unwrap();
                let accepted = stream.into_std().unwrap();
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TlsTransport::accept(
                            &context,
                            accepted,
                            server_options,
                            ByteStreamLimits::new(64, 256 * 1024, 8).unwrap(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        let owner = transport
                            .zero_copy_owner_for_test(&context)
                            .unwrap()
                            .expect("accepted TLS has one TCP completion owner");
                        let enabled = transport.enable_zero_copy(&context, 32 * 1024).unwrap();
                        assert_eq!(
                            transport.enable_zero_copy(&context, 64 * 1024).unwrap(),
                            enabled,
                        );
                        assert_eq!(
                            transport.zero_copy_owner_for_test(&context).unwrap(),
                            Some(owner),
                        );
                        assert_eq!(
                            transport.connection_info().key_exchange_group(),
                            Some("X25519"),
                        );
                        let stream = transport.into_byte_stream();
                        let request = stream
                            .read(
                                &context,
                                4,
                                4,
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(read_bytes(request), b"ping");
                        let first = vec![b'a'; 64 * 1024];
                        let second = vec![b'b'; 64 * 1024];
                        let written = stream
                            .write(
                                &context,
                                queue(&[&first, &second]),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(written.bytes_written, 128 * 1024);
                        stream.close(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("accepted TLS I/O must remain bounded")
                    .unwrap();
                time::timeout(Duration::from_secs(5), client)
                    .await
                    .expect("TLS client must observe the response")
                    .unwrap();
                worker.begin_shutdown();
                time::timeout(Duration::from_secs(5), driver_task)
                    .await
                    .expect("worker shutdown must remain bounded")
                    .unwrap();
            }));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn tls_real_rustls_write_times_out_when_ciphertext_staging_blocks() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (certificate, server_config) = tls_fixture();
                let socket = TcpSocket::new_v4().unwrap();
                socket.set_recv_buffer_size(4096).unwrap();
                socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let listener = socket.listen(1).unwrap();
                let destination = listener.local_addr().unwrap();
                let (release_tx, release_rx) = tokio::sync::oneshot::channel();
                let server = tokio::task::spawn_local(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let _tls = TlsAcceptor::from(server_config)
                        .accept(stream)
                        .await
                        .unwrap();
                    let _ = release_rx.await;
                });
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let mut options = TlsClientOptions::new("localhost");
                        options.add_root_certificate(certificate);
                        let tcp_options = TcpConnectOptions {
                            bind_address: None,
                            socket_options: vec![TcpSocketOption {
                                level: libc::SOL_SOCKET,
                                name: libc::SO_SNDBUF,
                                value: TcpSocketOptionValue::Integer(4096),
                                phase: TcpSocketOptionPhase::PostConnect,
                            }],
                            congestion_flavor: None,
                        };
                        let transport = TlsTransport::connect_with_tcp_options(
                            &context,
                            address(destination),
                            options,
                            ByteStreamLimits::new(64, 256 * 1024, 8).unwrap(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                            tcp_options,
                        )
                        .await
                        .unwrap();
                        let payload = vec![b'x'; 256 * 1024];
                        let error = transport
                            .write(
                                &context,
                                BufQueue::from_bytes(payload.into(), 256 * 1024).unwrap(),
                                Duration::from_millis(100),
                                CancellationToken::never(),
                            )
                            .await
                            .expect_err(
                                "TLS write must block behind bounded socket and staging buffers",
                            );
                        assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                        assert!(error.bytes_written() < 256 * 1024);
                        let _ = release_tx.send(());
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                time::timeout(Duration::from_secs(5), call)
                    .await
                    .expect("blocked real-rustls write must time out")
                    .unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
                server.await.unwrap();
            }));
    }

    fn zero_copy_controller(threshold: usize) -> TcpZeroCopy {
        let mut controller = TcpZeroCopy::new();
        assert!(controller.enable_for_test(threshold));
        controller
    }

    fn submit_zero_copy(controller: &mut TcpZeroCopy, data: &BufQueue, written: usize) -> u32 {
        let cookie = controller.next_cookie_for_test();
        let (outcome, calls) = controller.write_after_ready_for_test(data, true, [Ok(written)]);
        assert!(matches!(
            outcome,
            SendOutcome::Written(amount) if amount == written
        ));
        assert_eq!(calls, vec![true]);
        cookie
    }

    #[test]
    fn zero_copy_completion_before_registration() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"completion-race"]);

        assert!(matches!(
            controller.submit_for_test(
                &data,
                Ok(data.chain_length()),
                Some((0, 0, false)),
            ),
            SendOutcome::Written(amount) if amount == data.chain_length()
        ));
        assert_eq!(controller.pending_count_for_test(), 0);
        assert_eq!(controller.next_cookie_for_test(), 1);
    }

    #[test]
    fn zero_copy_repeated_enable_preserves_pending_state() {
        let mut controller = zero_copy_controller(4);
        let data = queue(&[b"pending"]);
        assert_eq!(submit_zero_copy(&mut controller, &data, 7), 0);

        let owner = controller.completion_owner_for_test();
        assert!(controller.enable_for_test(32));
        assert_eq!(controller.completion_owner_for_test(), owner);
        assert_eq!(controller.pending_count_for_test(), 1);
        assert_eq!(controller.next_cookie_for_test(), 1);
        assert!(!controller.begin_write(31));
        assert!(controller.begin_write(32));
    }

    #[test]
    fn zero_copy_partial_writes_share_allocation() {
        let first = Vec::from(&b"first"[..]);
        let second = Vec::from(&b"second-segment"[..]);
        let third = Vec::from(&b"third"[..]);
        let first_pointer = first.as_ptr();
        let second_pointer = second.as_ptr();
        let mut data = BufQueue::new(first.len() + second.len() + third.len());
        data.push_vec(first).expect("first segment fits");
        data.push_vec(second).expect("second segment fits");
        data.push_vec(third).expect("third segment fits");
        let mut controller = zero_copy_controller(1);

        submit_zero_copy(&mut controller, &data, 8);
        assert_eq!(
            controller.pending_segments_for_test(0),
            vec![(first_pointer, 5), (second_pointer, 3)],
        );
    }

    #[test]
    fn zero_copy_completion_range_releases_once() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"range"]);
        (0..3).for_each(|cookie| {
            assert_eq!(submit_zero_copy(&mut controller, &data, 5), cookie);
        });

        let wake = Arc::new(WakeSignal::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        assert!(controller.poll_drain(&mut context).is_pending());
        assert_eq!(controller.complete_for_test(0, 2, false), 3);
        assert!(wake.take(), "the idle completion consumer wakes the drain");
        assert_eq!(controller.complete_for_test(0, 2, false), 0);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert!(controller.poll_drain(&mut context).is_ready());
    }

    #[test]
    fn zero_copy_cookie_wraparound_releases_once() {
        let mut controller = zero_copy_controller(1);
        controller.set_next_cookie_for_test(u32::MAX - 1);
        let data = queue(&[b"wrap"]);
        [u32::MAX - 1, u32::MAX, 0]
            .into_iter()
            .for_each(|cookie| assert_eq!(submit_zero_copy(&mut controller, &data, 4), cookie));

        assert_eq!(controller.complete_for_test(u32::MAX, 0, false), 2);
        assert_eq!(controller.pending_count_for_test(), 1);
        assert_eq!(controller.complete_for_test(u32::MAX, 0, false), 0);
        assert_eq!(
            controller.complete_for_test(u32::MAX - 1, u32::MAX - 1, false),
            1,
        );
    }

    #[test]
    fn zero_copy_enobufs_retries_and_reenables() {
        let mut controller = zero_copy_controller(1);
        controller.set_reenable_threshold(2);
        assert!(!controller.allows_borrowed_write());
        let data = queue(&[b"retry"]);

        let (outcome, calls) = controller.write_after_ready_for_test(
            &data,
            true,
            [
                Err(io::Error::from_raw_os_error(libc::ENOBUFS)),
                Ok(data.chain_length()),
            ],
        );
        assert!(matches!(
            outcome,
            SendOutcome::Written(amount) if amount == data.chain_length()
        ));
        assert_eq!(calls, vec![true, false]);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert_eq!(controller.next_cookie_for_test(), 0);
        assert!(!controller.allows_borrowed_write());
        assert!(!controller.begin_write(5));
        assert!(!controller.allows_borrowed_write());
        assert!(controller.begin_write(5));
        assert!(!controller.allows_borrowed_write());
        assert!(!controller.enable_for_test(0));
        assert!(controller.allows_borrowed_write());
        controller.force_teardown();
        assert!(!controller.allows_borrowed_write());
    }

    #[test]
    fn zero_copy_copied_completion_releases_and_disables() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"copied"]);
        submit_zero_copy(&mut controller, &data, 6);

        assert_eq!(controller.complete_for_test(0, 0, true), 1);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert!(!controller.is_enabled_for_test());
        assert!(!controller.begin_write(6));
        assert!(controller.enable_for_test(1));
    }

    #[test]
    fn zero_copy_eagain_waits_without_state_change() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"again"]);

        let (outcome, calls) = controller.write_after_ready_for_test(
            &data,
            true,
            [Err(io::ErrorKind::WouldBlock.into())],
        );
        assert!(matches!(outcome, SendOutcome::Wait));
        assert_eq!(calls, vec![true]);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert_eq!(controller.next_cookie_for_test(), 0);
        assert!(controller.is_enabled_for_test());
    }

    #[test]
    fn zero_copy_other_send_errors_propagate() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"error"]);

        let (outcome, calls) = controller.write_after_ready_for_test(
            &data,
            true,
            [Err(io::Error::from_raw_os_error(libc::EPIPE))],
        );
        assert!(matches!(
            outcome,
            SendOutcome::Error(error) if error.raw_os_error() == Some(libc::EPIPE)
        ));
        assert_eq!(calls, vec![true]);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert_eq!(controller.next_cookie_for_test(), 0);
        assert!(controller.is_enabled_for_test());
    }

    #[test]
    fn zero_copy_connected_socket_uses_one_completion_owner() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let peer = tokio::task::spawn_local(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut payload = vec![0; 128 * 1024];
                    stream.read_exact(&mut payload).await.unwrap();
                    assert!(payload.iter().all(|byte| *byte == b'z'));
                });
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::connect(
                            &context,
                            address(destination),
                            ByteStreamLimits::new(64, 256 * 1024, 8).unwrap(),
                            Duration::from_secs(1),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                        let owner = transport
                            .zero_copy_owner_for_test(&context)
                            .unwrap()
                            .expect("plaintext TCP has a completion owner");
                        let enabled = transport.enable_zero_copy(&context, 32_768).unwrap();
                        let enabled_again =
                            transport.enable_zero_copy(&context, 64 * 1024).unwrap();
                        assert_eq!(enabled_again, enabled);
                        assert_eq!(
                            transport.zero_copy_owner_for_test(&context).unwrap(),
                            Some(owner),
                        );
                        let payload = vec![b'z'; 128 * 1024];
                        transport
                            .write(
                                &context,
                                BufQueue::from_vec(payload, 128 * 1024)
                                    .expect("payload fits transport write bound"),
                                Duration::from_secs(1),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        time::timeout(Duration::from_secs(1), async {
                            while transport.zero_copy_pending_for_test(&context).unwrap() != 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .expect("the idle completion pump releases submitted backing");
                        transport.close(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
                peer.await.unwrap();
            }));
    }

    #[test]
    fn zero_copy_accepted_socket_uses_one_completion_owner() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                let client = tokio::task::spawn_local(TcpStream::connect(destination));
                let (stream, _) = listener.accept().await.unwrap();
                let accepted = stream.into_std().unwrap();
                let mut client = client.await.unwrap().unwrap();
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let call = worker
                    .call_local(move |context| async move {
                        let transport = TcpTransport::from_accepted(&context, accepted, limits())
                            .expect("accepted stream is adopted");
                        let owner = transport
                            .zero_copy_owner_for_test(&context)
                            .unwrap()
                            .expect("accepted TCP has a completion owner");
                        transport.enable_zero_copy(&context, 32_768).unwrap();
                        transport.enable_zero_copy(&context, 64 * 1024).unwrap();
                        assert_eq!(
                            transport.zero_copy_owner_for_test(&context).unwrap(),
                            Some(owner),
                        );
                        transport.close(&context).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
                let mut byte = [0; 1];
                assert_eq!(client.read(&mut byte).await.unwrap(), 0);
            }));
    }

    #[test]
    fn zero_copy_sockets_do_not_share_completion_state() {
        let mut first = zero_copy_controller(1);
        let mut second = zero_copy_controller(1);
        assert_ne!(
            first.completion_owner_for_test(),
            second.completion_owner_for_test(),
        );
        let data = queue(&[b"isolated"]);
        submit_zero_copy(&mut first, &data, 8);
        submit_zero_copy(&mut second, &data, 8);

        assert_eq!(first.complete_for_test(0, 0, false), 1);
        assert_eq!(first.pending_count_for_test(), 0);
        assert_eq!(second.pending_count_for_test(), 1);
    }

    #[test]
    fn zero_copy_sliced_backing_survives_completion() {
        let backing = Vec::from(&b"prefix-visible-suffix"[..]);
        let expected = backing.as_ptr().wrapping_add(7);
        let mut full = BufQueue::from_vec(backing, 21).expect("backing fits");
        full.trim_start(7).expect("prefix exists");
        let mut controller = zero_copy_controller(1);

        submit_zero_copy(&mut controller, &full, 7);
        assert_eq!(controller.pending_segments_for_test(0), vec![(expected, 7)],);
        assert_eq!(controller.complete_for_test(0, 0, false), 1);
    }

    #[test]
    fn zero_copy_close_reset_cancel_retains_until_teardown() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let abandoned = Arc::new(AtomicBool::new(false));
                let call_abandoned = Arc::clone(&abandoned);
                let call = worker
                    .call_local(move |context| async move {
                        let (transport, controller) = zero_copy_lifecycle_transport(
                            &context,
                            [Ok(4), Err(io::ErrorKind::WouldBlock.into())],
                            call_abandoned,
                            Arc::new(AtomicBool::new(false)),
                        );
                        assert!(transport.enable_zero_copy(&context, 1).unwrap());
                        let backing = Vec::from(&b"cancelled"[..]);
                        let pointer = backing.as_ptr();
                        let data = BufQueue::from_vec(backing, 9).expect("backing fits");
                        let cancellation = CancellationSource::new();
                        let write =
                            transport.write(&context, data, Duration::ZERO, cancellation.token());
                        tokio::pin!(write);
                        std::future::poll_fn(|task_context| {
                            match write.as_mut().poll(task_context) {
                                Poll::Pending => Poll::Ready(()),
                                Poll::Ready(result) => {
                                    panic!("write completed before cancellation: {result:?}")
                                }
                            }
                        })
                        .await;
                        assert_eq!(controller.borrow().pending_count_for_test(), 1);
                        assert!(cancellation.request_cancellation());
                        let error = write.await.expect_err("cancelled write fails");
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                        assert_eq!(error.bytes_written(), 4);
                        assert!(controller.borrow().is_terminal_for_test());
                        assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                assert!(abandoned.load(Ordering::Acquire));
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn zero_copy_graceful_close_waits_for_completion() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let shutdown_complete = Arc::new(AtomicBool::new(false));
                let call_shutdown = Arc::clone(&shutdown_complete);
                let call = worker
                    .call_local(move |context| async move {
                        let (transport, controller) = zero_copy_lifecycle_transport(
                            &context,
                            [Ok(8)],
                            Arc::new(AtomicBool::new(false)),
                            call_shutdown,
                        );
                        assert!(transport.enable_zero_copy(&context, 1).unwrap());
                        transport
                            .write(
                                &context,
                                queue(&[b"graceful"]),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(controller.borrow().pending_count_for_test(), 1);

                        transport.close(&context).unwrap();
                        tokio::task::yield_now().await;
                        assert!(!shutdown_complete.load(Ordering::Acquire));
                        assert_eq!(controller.borrow_mut().complete_for_test(0, 0, false), 1);
                        while !shutdown_complete.load(Ordering::Acquire) {
                            tokio::task::yield_now().await;
                        }
                        drop(transport);
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn zero_copy_reset_quarantines_submitted_backing() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let abandoned = Arc::new(AtomicBool::new(false));
                let call_abandoned = Arc::clone(&abandoned);
                let call = worker
                    .call_local(move |context| async move {
                        let (transport, controller) = zero_copy_lifecycle_transport(
                            &context,
                            [Ok(5)],
                            call_abandoned,
                            Arc::new(AtomicBool::new(false)),
                        );
                        assert!(transport.enable_zero_copy(&context, 1).unwrap());
                        let backing = Vec::from(&b"reset"[..]);
                        let pointer = backing.as_ptr();
                        transport
                            .write(
                                &context,
                                BufQueue::from_vec(backing, 5).expect("backing fits"),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        transport.close_with_reset(&context).unwrap();
                        assert!(controller.borrow().is_terminal_for_test());
                        assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                assert!(abandoned.load(Ordering::Acquire));
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn zero_copy_drop_quarantines_submitted_backing() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let abandoned = Arc::new(AtomicBool::new(false));
                let call_abandoned = Arc::clone(&abandoned);
                let call = worker
                    .call_local(move |context| async move {
                        let (transport, controller) = zero_copy_lifecycle_transport(
                            &context,
                            [Ok(4)],
                            call_abandoned,
                            Arc::new(AtomicBool::new(false)),
                        );
                        assert!(transport.enable_zero_copy(&context, 1).unwrap());
                        let backing = Vec::from(&b"drop"[..]);
                        let pointer = backing.as_ptr();
                        transport
                            .write(
                                &context,
                                BufQueue::from_vec(backing, 4).expect("backing fits"),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        drop(transport);
                        assert!(controller.borrow().is_terminal_for_test());
                        assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                assert!(abandoned.load(Ordering::Acquire));
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    #[test]
    fn zero_copy_worker_cleanup_quarantines_submitted_backing() {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let abandoned = Arc::new(AtomicBool::new(false));
                let call_abandoned = Arc::clone(&abandoned);
                let call = worker
                    .call_local(move |context| async move {
                        let (transport, _) = zero_copy_lifecycle_transport(
                            &context,
                            [Ok(7)],
                            call_abandoned,
                            Arc::new(AtomicBool::new(false)),
                        );
                        assert!(transport.enable_zero_copy(&context, 1).unwrap());
                        let backing = Vec::from(&b"cleanup"[..]);
                        let pointer = backing.as_ptr() as usize;
                        transport
                            .write(
                                &context,
                                BufQueue::from_vec(backing, 7).expect("backing fits"),
                                Duration::ZERO,
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        ESCAPED_DRAIN.with(|slot| slot.replace(Some(transport)));
                        pointer
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                let pointer = call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
                assert!(abandoned.load(Ordering::Acquire));
                assert!(TcpZeroCopy::is_quarantined_for_test(pointer as *const u8));
                ESCAPED_DRAIN.with(|slot| slot.borrow_mut().take());
            }));
    }

    #[test]
    fn zero_copy_live_cookie_collision_uses_ordinary_write() {
        let mut controller = zero_copy_controller(1);
        let data = queue(&[b"collision"]);
        submit_zero_copy(&mut controller, &data, 9);
        controller.set_next_cookie_for_test(0);

        let (outcome, calls) =
            controller.write_after_ready_for_test(&data, true, [Ok(data.chain_length())]);
        assert!(matches!(outcome, SendOutcome::Written(9)));
        assert_eq!(calls, vec![false]);
        assert_eq!(controller.pending_count_for_test(), 1);
        assert_eq!(controller.next_cookie_for_test(), 0);
    }

    #[test]
    fn zero_copy_malformed_completion_quarantines_pending_backing() {
        #[cfg(target_os = "linux")]
        assert!(TcpZeroCopy::completion_parser_validation_for_test());
        [false, true].into_iter().for_each(|truncated| {
            let backing = Vec::from(&b"malformed"[..]);
            let pointer = backing.as_ptr();
            let data = BufQueue::from_vec(backing, 9).expect("backing fits");
            let mut controller = zero_copy_controller(1);
            submit_zero_copy(&mut controller, &data, 9);

            #[cfg(target_os = "linux")]
            controller.drain_malformed_for_test(truncated);
            #[cfg(not(target_os = "linux"))]
            {
                let _ = truncated;
                controller.force_teardown();
            }
            assert!(controller.is_terminal_for_test());
            assert_eq!(controller.pending_count_for_test(), 0);
            assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
            assert!(!controller.enable_for_test(1));
            assert!(!controller.begin_write(data.chain_length()));
            let (outcome, calls) =
                controller.write_after_ready_for_test(&data, true, [Ok(data.chain_length())]);
            assert!(matches!(outcome, SendOutcome::Written(9)));
            assert_eq!(calls, vec![false]);
        });
    }

    #[test]
    fn zero_copy_abandoned_preparation_quarantines_backing() {
        let backing = Vec::from(&b"panic-guard"[..]);
        let pointer = backing.as_ptr();
        let data = BufQueue::from_vec(backing, 11).expect("backing fits");
        let mut controller = zero_copy_controller(1);

        controller.abandon_prepared_send_for_test(&data);
        assert_eq!(controller.pending_count_for_test(), 0);
        assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
    }

    #[test]
    fn zero_copy_inherited_socket_option_fallback_is_best_effort() {
        let (enabled, set_calls, get_calls) = TcpZeroCopy::inherited_enable_fallback_for_test();

        assert!(enabled);
        assert_eq!(set_calls, 2);
        assert_eq!(get_calls, 1);
    }

    #[test]
    fn zero_copy_below_threshold_uses_only_ordinary_write() {
        let mut controller = zero_copy_controller(16);
        let data = queue(&[b"short"]);
        let zero_copy = controller.begin_write(data.chain_length());

        let (outcome, calls) =
            controller.write_after_ready_for_test(&data, zero_copy, [Ok(data.chain_length())]);
        assert!(matches!(outcome, SendOutcome::Written(5)));
        assert_eq!(calls, vec![false]);
        assert_eq!(controller.pending_count_for_test(), 0);
    }

    type TransportCancellation = (Option<StreamId>, ByteEventType, HttpErrorCode, Vec<u8>);

    #[derive(Default)]
    struct TransportMessageLog {
        cancelled: RefCell<Vec<TransportCancellation>>,
    }

    impl HttpByteEventCallback for TransportMessageLog {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled.borrow_mut().push((
                event.stream_id,
                event.event_type,
                error.code(),
                error.message().to_vec(),
            ));
        }
    }

    fn transport_cancellation(
        stream: u64,
        event_type: ByteEventType,
        code: HttpErrorCode,
        message: &[u8],
    ) -> TransportCancellation {
        (
            Some(StreamId::new(stream)),
            event_type,
            code,
            message.to_vec(),
        )
    }

    /// The cancellations of the byte events that
    /// `transport_with_stream_byte_events` leaves in the stream, in order.
    fn stream_cancellations(code: HttpErrorCode, message: &[u8]) -> Vec<TransportCancellation> {
        [1, 3]
            .into_iter()
            .map(|stream| {
                transport_cancellation(stream, ByteEventType::CumulativeAck, code, message)
            })
            .collect()
    }

    fn drain_transport(
        context: &roxygen_runtime::worker::WorkerContext,
        worker: &WorkerHandle,
        control: &DrainControl,
    ) -> OwnedByteStream {
        control.allow_write();
        owned_from_stream(
            context,
            DrainStream {
                control: control.clone(),
                worker: worker.clone(),
            },
            SocketAddress::default(),
            SocketAddress::default(),
            None,
            limits(),
        )
        .unwrap()
    }

    /// A native transport whose stream holds the byte events of two completed
    /// writes, the first of them partly fired, and, when `unbound`, one more
    /// registration that no write has taken.
    async fn transport_with_stream_byte_events(
        context: &roxygen_runtime::worker::WorkerContext,
        worker: &WorkerHandle,
        control: &DrainControl,
        log: &Rc<TransportMessageLog>,
        unbound: bool,
    ) -> OwnedByteStream {
        control.allow_write();
        let events = CapableEventControl::default();
        let transport = owned_from_stream(
            context,
            CapableDrainStream {
                inner: DrainStream {
                    control: control.clone(),
                    worker: worker.clone(),
                },
                events: events.clone(),
            },
            SocketAddress::default(),
            SocketAddress::default(),
            None,
            limits(),
        )
        .unwrap();
        for (stream, bits, flags, payload) in [
            (
                1,
                ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                ByteEventWriteFlags {
                    timestamp_write: true,
                    timestamp_tx: true,
                    timestamp_ack: true,
                },
                &b"ping"[..],
            ),
            (
                3,
                ByteEventType::CumulativeAck.bit(),
                ByteEventWriteFlags {
                    timestamp_write: true,
                    timestamp_tx: false,
                    timestamp_ack: true,
                },
                &b"pong"[..],
            ),
        ] {
            transport
                .register_byte_event(
                    context,
                    HttpByteEventRegistration::new(Some(StreamId::new(stream)), bits, log),
                )
                .unwrap();
            transport
                .write_with_byte_events(
                    context,
                    queue(&[payload]),
                    flags,
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .unwrap();
        }
        events.notify(ByteEventType::NicTx, 8);
        if unbound {
            transport
                .register_byte_event(
                    context,
                    HttpByteEventRegistration::new(
                        Some(StreamId::new(5)),
                        ByteEventType::TransportWrite.bit(),
                        log,
                    ),
                )
                .unwrap();
        }
        assert!(log.cancelled.borrow().is_empty());
        transport
    }

    async fn wait_for_shutdown_pending(control: &DrainControl) {
        control
            .wait_for_action(|action| matches!(action, DrainAction::ShutdownPending(..)))
            .await;
    }

    async fn wait_for_stream_drop(control: &DrainControl) {
        control
            .wait_for_action(|action| matches!(action, DrainAction::Drop(..)))
            .await;
    }

    fn on_drain_worker(
        test: impl FnOnce(
            roxygen_runtime::worker::WorkerContext,
            WorkerHandle,
        ) -> LocalTransportFuture<()>
        + Send
        + 'static,
    ) {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async move {
                time::timeout(Duration::from_secs(5), async move {
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let stream_worker = worker.clone();
                    let call = worker
                        .call_local(move |context| test(context, stream_worker))
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                })
                .await
                .expect("native close paths finish within their bound");
            }));
    }

    #[test]
    fn tcp_retire_and_drain_cancel_byte_events_with_unchanged_codes_and_messages() {
        const TCP_RESET: &[u8] = b"transport reset cancelled pending byte event";
        const TCP_DROPPED: &[u8] = b"TCP transport owner was dropped";
        const TCP_CLOSED: &[u8] = b"transport closed before byte event";
        const RESET: &[u8] = b"byte-stream reset cancelled byte event";
        const DROPPED: &[u8] = b"byte-stream transport was dropped";
        const CLOSED: &[u8] = b"byte-stream closed before byte event";
        const SHUT_DOWN: &[u8] = b"byte-stream write side was shut down before byte event";
        on_drain_worker(|context, worker| {
            Box::pin(async move {
                // Reset: the stream's byte events, then the unbound registration.
                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, true)
                        .await;
                transport.close_with_reset(&context).unwrap();
                let mut expected =
                    stream_cancellations(HttpErrorCode::TRANSPORT_WRITE_ERROR, TCP_RESET);
                expected.push(transport_cancellation(
                    5,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    RESET,
                ));
                assert_eq!(*log.cancelled.borrow(), expected);
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(*log.cancelled.borrow(), expected);

                // Drop: the stream's byte events, then the unbound registration.
                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, true)
                        .await;
                drop(transport);
                let mut expected = stream_cancellations(HttpErrorCode::DROPPED, TCP_DROPPED);
                expected.push(transport_cancellation(
                    5,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::DROPPED,
                    DROPPED,
                ));
                assert_eq!(*log.cancelled.borrow(), expected);
                wait_for_stream_drop(&control).await;
                assert_eq!(*log.cancelled.borrow(), expected);

                // Drop while a half-close drains: the drain keeps the stream,
                // and the drop cancels its byte events.
                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, true)
                        .await;
                transport.shutdown_write(&context).unwrap();
                let mut expected = vec![transport_cancellation(
                    5,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    SHUT_DOWN,
                )];
                assert_eq!(*log.cancelled.borrow(), expected);
                wait_for_shutdown_pending(&control).await;
                drop(transport);
                expected.extend(stream_cancellations(HttpErrorCode::DROPPED, TCP_DROPPED));
                assert_eq!(*log.cancelled.borrow(), expected);
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                assert_eq!(*log.cancelled.borrow(), expected);

                // Graceful close: the unbound registration at once, the
                // stream's byte events when the drain retires the stream.
                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, true)
                        .await;
                transport.close(&context).unwrap();
                let mut expected = vec![transport_cancellation(
                    5,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                )];
                assert_eq!(*log.cancelled.borrow(), expected);
                wait_for_shutdown_pending(&control).await;
                assert_eq!(*log.cancelled.borrow(), expected);
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                expected.extend(stream_cancellations(
                    HttpErrorCode::CORO_CANCELLED,
                    TCP_CLOSED,
                ));
                assert_eq!(*log.cancelled.borrow(), expected);
                drop(transport);
                assert_eq!(*log.cancelled.borrow(), expected);

                // Close after a completed half-close retires the stream at once.
                let control = DrainControl::default();
                control.permit_shutdowns(1);
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, true)
                        .await;
                transport.shutdown_write(&context).unwrap();
                control
                    .wait_for_action(|action| matches!(action, DrainAction::ShutdownReady(..)))
                    .await;
                let mut expected = vec![transport_cancellation(
                    5,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    SHUT_DOWN,
                )];
                assert_eq!(*log.cancelled.borrow(), expected);
                transport.close(&context).unwrap();
                expected.extend(stream_cancellations(
                    HttpErrorCode::CORO_CANCELLED,
                    TCP_CLOSED,
                ));
                assert_eq!(*log.cancelled.borrow(), expected);
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(*log.cancelled.borrow(), expected);
            })
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_io_destruction_keeps_its_phase_on_graceful_shutdown_and_reset_paths() {
        #[derive(Clone, Copy, Debug)]
        enum Path {
            Graceful,
            ShutdownWriteThenDrop,
            Reset,
        }

        fn destroyed(state: &Rc<RefCell<RetirementProbeState>>) -> (usize, usize) {
            let state = state.borrow();
            (state.body_drops, state.field_drops)
        }

        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                time::timeout(
                Duration::from_secs(5),
                run_no_delay_worker(|context| {
                    Box::pin(async move {
                        for path in [Path::Graceful, Path::ShutdownWriteThenDrop, Path::Reset] {
                            let state = Rc::new(RefCell::new(RetirementProbeState::default()));
                            let transport = owned_from_stream(
                                &context,
                                RetirementProbe {
                                    field: RetirementField {
                                        state: Rc::clone(&state),
                                        owner: std::thread::current().id(),
                                    },
                                },
                                SocketAddress::default(),
                                SocketAddress::default(),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let receipt = transport.retirement().unwrap();
                            state.borrow_mut().receipt = Some(receipt.clone());
                            let mut observing = Box::pin(receipt.wait_retired());
                            assert!(poll_once(observing.as_mut()).is_pending());
                            match path {
                                Path::Graceful => {
                                    transport.close(&context).unwrap();
                                    retirement_probe_phase(&state, |state| state.flush_polled)
                                        .await;
                                    assert_eq!(destroyed(&state), (0, 0), "{path:?}");
                                    release_retirement_probe(&state, false);
                                    retirement_probe_phase(&state, |state| state.shutdown_polled)
                                        .await;
                                    assert_eq!(destroyed(&state), (0, 0), "{path:?}");
                                    assert!(poll_once(observing.as_mut()).is_pending());
                                    release_retirement_probe(&state, true);
                                    assert_eq!(observing.await, Ok(()));
                                    assert_eq!(
                                        destroyed(&state),
                                        (1, 1),
                                        "the drain destroys the I/O when its shutdown completes, \
                                         before the handle drops",
                                    );
                                    drop(transport);
                                }
                                Path::ShutdownWriteThenDrop => {
                                    transport.shutdown_write(&context).unwrap();
                                    retirement_probe_phase(&state, |state| state.flush_polled)
                                        .await;
                                    release_retirement_probe(&state, false);
                                    retirement_probe_phase(&state, |state| state.shutdown_polled)
                                        .await;
                                    drop(transport);
                                    assert_eq!(
                                        destroyed(&state),
                                        (0, 0),
                                        "the half-close drain keeps the I/O after the handle drops",
                                    );
                                    assert!(poll_once(observing.as_mut()).is_pending());
                                    release_retirement_probe(&state, true);
                                    assert_eq!(observing.await, Ok(()));
                                    assert_eq!(destroyed(&state), (1, 1), "{path:?}");
                                }
                                Path::Reset => {
                                    transport.close_with_reset(&context).unwrap();
                                    assert_eq!(
                                        destroyed(&state),
                                        (1, 1),
                                        "reset destroys the I/O before it returns",
                                    );
                                    assert_eq!(observing.await, Ok(()));
                                    drop(transport);
                                }
                            }
                            assert_eq!(destroyed(&state), (1, 1), "{path:?}");
                            assert_eq!(receipt.wait_retired().await, Ok(()), "{path:?}");
                        }
                    })
                }),
            )
            .await
            .expect("bounded native close scopes and owned values");
            });
    }

    #[test]
    fn tcp_close_paths_without_byte_events_build_no_error() {
        on_drain_worker(|context, worker| {
            Box::pin(async move {
                let errors = close_path_errors_for_test;

                // A native stream without byte events.
                let control = DrainControl::default();
                let transport = drain_transport(&context, &worker, &control);
                let before = errors();
                transport.close_with_reset(&context).unwrap();
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before, "native reset");

                let control = DrainControl::default();
                let transport = drain_transport(&context, &worker, &control);
                let before = errors();
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before, "native drop");

                let control = DrainControl::default();
                let transport = drain_transport(&context, &worker, &control);
                let before = errors();
                transport.shutdown_write(&context).unwrap();
                wait_for_shutdown_pending(&control).await;
                drop(transport);
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before, "native drop during a half-close drain");

                let control = DrainControl::default();
                let transport = drain_transport(&context, &worker, &control);
                let before = errors();
                transport.close(&context).unwrap();
                wait_for_shutdown_pending(&control).await;
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                drop(transport);
                assert_eq!(errors(), before, "native graceful close");

                let control = DrainControl::default();
                control.permit_shutdowns(1);
                let transport = drain_transport(&context, &worker, &control);
                let before = errors();
                transport.shutdown_write(&context).unwrap();
                control
                    .wait_for_action(|action| matches!(action, DrainAction::ShutdownReady(..)))
                    .await;
                transport.close(&context).unwrap();
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(
                    errors(),
                    before,
                    "native close after a completed half-close"
                );

                // A real loopback TCP stream.
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let destination = listener.local_addr().unwrap();
                for path in 0..3 {
                    let transport = TcpTransport::connect(
                        &context,
                        address(destination),
                        limits(),
                        Duration::from_secs(1),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                    let (_peer, _) = listener.accept().await.unwrap();
                    let receipt = transport.retirement().unwrap();
                    let before = errors();
                    match path {
                        0 => transport.close_with_reset(&context).unwrap(),
                        1 => transport.close(&context).unwrap(),
                        _ => transport.shutdown_write(&context).unwrap(),
                    }
                    drop(transport);
                    assert_eq!(receipt.wait_retired().await, Ok(()));
                    assert_eq!(errors(), before, "loopback TCP path {path}");
                }

                // An adopted stream.
                for path in 0..4 {
                    let (io, _peer) = tokio::io::duplex(64);
                    let transport = adopt_local(
                        &context,
                        EstablishedIo::new(
                            io,
                            TransportEndpoint::Unknown,
                            TransportEndpoint::Unknown,
                            None,
                        ),
                        limits(),
                    )
                    .unwrap();
                    let receipt = transport.retirement().unwrap();
                    let before = errors();
                    match path {
                        0 => transport.close_with_reset(&context).unwrap(),
                        1 => {}
                        2 => transport.close(&context).unwrap(),
                        _ => transport.shutdown_write(&context).unwrap(),
                    }
                    drop(transport);
                    assert_eq!(receipt.wait_retired().await, Ok(()));
                    assert_eq!(errors(), before, "adopted path {path}");
                }

                // With byte events in the stream, each path builds its error once.
                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, false)
                        .await;
                let before = errors();
                transport.close_with_reset(&context).unwrap();
                assert_eq!(errors(), before + 1, "native reset");
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before + 1, "native reset");

                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, false)
                        .await;
                let before = errors();
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before + 1, "native drop");

                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, false)
                        .await;
                let before = errors();
                transport.shutdown_write(&context).unwrap();
                wait_for_shutdown_pending(&control).await;
                assert_eq!(errors(), before);
                drop(transport);
                assert_eq!(
                    errors(),
                    before + 1,
                    "native drop during a half-close drain"
                );
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                assert_eq!(
                    errors(),
                    before + 1,
                    "native drop during a half-close drain"
                );

                let control = DrainControl::default();
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, false)
                        .await;
                let before = errors();
                transport.close(&context).unwrap();
                wait_for_shutdown_pending(&control).await;
                assert_eq!(errors(), before);
                control.permit_shutdowns(1);
                wait_for_stream_drop(&control).await;
                assert_eq!(errors(), before + 1, "native graceful close");
                drop(transport);
                assert_eq!(errors(), before + 1, "native graceful close");

                let control = DrainControl::default();
                control.permit_shutdowns(1);
                let log = Rc::new(TransportMessageLog::default());
                let transport =
                    transport_with_stream_byte_events(&context, &worker, &control, &log, false)
                        .await;
                let before = errors();
                transport.shutdown_write(&context).unwrap();
                control
                    .wait_for_action(|action| matches!(action, DrainAction::ShutdownReady(..)))
                    .await;
                assert_eq!(errors(), before);
                transport.close(&context).unwrap();
                assert_eq!(
                    errors(),
                    before + 1,
                    "native close after a completed half-close"
                );
                drop(transport);
                wait_for_stream_drop(&control).await;
                assert_eq!(
                    errors(),
                    before + 1,
                    "native close after a completed half-close"
                );
            })
        });
    }

    mod native_write_queue {
        use roxygen_runtime::worker::WorkerContext;

        use super::*;
        use crate::byte_stream::write_deadline_batons_for_test;
        use crate::byte_stream::write_operations_for_test;
        use crate::byte_stream::write_progress_cells_for_test;
        use crate::byte_stream::write_registrations_for_test;
        use crate::tcp::io_write_futures_for_test;
        use crate::tcp_tls_tests::tcp_tls::owned_reader::with_observed_tcp;
        #[cfg(target_os = "linux")]
        use crate::tcp_zero_copy::send_iovec_arrays_for_test;

        /// Runs `test` on a worker with an accepted TCP transport bounded by `limits` and the
        /// connecting peer's Tokio stream.
        pub(super) fn with_accepted_tcp<F, Fut>(limits: ByteStreamLimits, test: F)
        where
            F: FnOnce(WorkerContext, OwnedByteStream, TcpStream) -> Fut + Send + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let accepted = accepted.into_std().unwrap();
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport =
                                TcpTransport::from_accepted(&context, accepted, limits).unwrap();
                            test(context, transport, peer).await;
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(10), call)
                        .await
                        .expect("the queue test is bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                }));
        }

        #[test]
        fn tcp_transport_offers_an_untimed_native_write_queue() {
            with_accepted_tcp(limits(), |context, transport, _peer| async move {
                let queue = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .expect("an untimed TCP transport offers its native write queue");
                assert!(queue.is_idle());
                assert!(
                    !queue.is_saturated(),
                    "an idle queue on an open transport is not saturated"
                );
                assert!(queue.can_write(1024));
                assert!(
                    transport
                        .write_queue(
                            &context,
                            1024,
                            Duration::from_secs(5),
                            CancellationToken::never(),
                        )
                        .unwrap()
                        .is_none(),
                    "a write timeout keeps the single-write API, which times each write",
                );
                drop(queue);
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn queued_writes_reach_the_peer_with_ordered_receipts() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                writer.start_write(BufQueue::new(0)).unwrap();
                writer.start_write(queue(&[b"first "])).unwrap();
                let receipts = [
                    poll_fn(|context| writer.poll_complete(context)).await,
                    poll_fn(|context| writer.poll_complete(context)).await,
                ];
                assert_eq!(
                    receipts
                        .iter()
                        .map(|receipt| (
                            receipt.requested_bytes,
                            receipt.result.as_ref().unwrap().bytes_written
                        ))
                        .collect::<Vec<_>>(),
                    [(0, 0), (6, 6)],
                    "the empty submission's receipt comes first, then the write's",
                );
                writer.start_write(queue(&[b"sec", b"ond"])).unwrap();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.requested_bytes, 6);
                assert_eq!(receipt.result.unwrap().bytes_written, 6);
                assert!(writer.is_idle());
                let mut received = [0; 12];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"first second");
                transport.close(&context).unwrap();
            });
        }

        /// More bytes than loopback socket buffers hold while the peer does not read.
        const STALLING_WRITE: usize = 16 << 20;

        /// Polls `writer` once.
        async fn poll_once(
            writer: &mut crate::byte_stream::OwnedWriteQueue,
        ) -> Poll<crate::byte_stream::QueuedWriteCompletion> {
            poll_fn(|context| Poll::Ready(writer.poll_complete(context))).await
        }

        /// Drives `writer`'s outstanding write until `peer` has received some of its bytes; the
        /// write stays outstanding because `peer` does not read.
        async fn stall_after_progress(
            writer: &mut crate::byte_stream::OwnedWriteQueue,
            peer: &TcpStream,
        ) {
            let mut byte = [0];
            tokio::select! {
                _ = poll_fn(|context| writer.poll_complete(context)) => {
                    panic!("a write larger than the socket buffers completes only as the peer reads")
                }
                peeked = peer.peek(&mut byte) => assert_eq!(peeked.unwrap(), 1),
            }
        }

        #[test]
        fn an_outstanding_write_holds_capacity_and_saturates_the_queue() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                let payload = pattern(STALLING_WRITE, 3);
                writer.start_write(queue(&[&payload])).unwrap();
                assert!(
                    writer.is_saturated(),
                    "a started write is outstanding until it is polled to completion"
                );
                assert!(!writer.can_write(0) && !writer.can_write(1));
                assert!(poll_once(&mut writer).await.is_pending());
                assert!(writer.is_saturated());
                assert!(!writer.can_write(1));
                assert!(!writer.is_idle());
                let reader = async {
                    let mut received = vec![0; STALLING_WRITE];
                    peer.read_exact(&mut received).await.unwrap();
                    received
                };
                let (receipt, received) =
                    tokio::join!(poll_fn(|context| writer.poll_complete(context)), reader);
                assert_eq!(receipt.requested_bytes, STALLING_WRITE);
                assert_eq!(receipt.result.unwrap().bytes_written, STALLING_WRITE);
                assert!(
                    received == payload,
                    "the peer receives the payload in order"
                );
                assert!(!writer.is_saturated());
                assert!(writer.can_write(1));
                assert!(writer.is_idle());
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn unconsumed_receipts_bound_the_queue() {
            with_accepted_tcp(limits(), |context, transport, _peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                for _ in 0..8 {
                    assert!(writer.can_write(0));
                    writer.start_write(BufQueue::new(0)).unwrap();
                }
                assert!(
                    !writer.can_write(0) && !writer.can_write(1),
                    "eight unconsumed receipts fill the queue"
                );
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.result.unwrap().bytes_written, 0);
                assert!(
                    writer.can_write(1),
                    "consuming a receipt returns its capacity"
                );
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_write_shut_transport_saturates_the_queue_and_fails_submissions() {
            with_accepted_tcp(limits(), |context, transport, _peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                transport.shutdown_write(&context).unwrap();
                assert!(
                    writer.is_saturated(),
                    "a transport whose write side is shut can no longer write"
                );
                assert!(
                    writer.can_write(5),
                    "capacity does not hide terminal transport state"
                );
                writer.start_write(queue(&[b"after"])).unwrap();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.requested_bytes, 5);
                let error = receipt.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(error.bytes_written(), 0, "a zero-byte failure receipt");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_reset_fails_the_outstanding_write_and_saturates_the_queue() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 5)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                transport.close_with_reset(&context).unwrap();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.requested_bytes, STALLING_WRITE);
                let error = receipt.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert!(
                    error.bytes_written() > 0 && error.bytes_written() < STALLING_WRITE,
                    "the receipt reports the bytes the socket took before the reset: {}",
                    error.bytes_written(),
                );
                assert!(
                    writer.is_saturated(),
                    "a reset transport can no longer write"
                );
                writer.start_write(queue(&[b"late"])).unwrap();
                let late = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(late.result.unwrap_err().bytes_written(), 0);
            });
        }

        #[test]
        fn a_graceful_close_finishes_the_outstanding_write_before_its_fin() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                let payload = pattern(STALLING_WRITE, 7);
                writer.start_write(queue(&[&payload])).unwrap();
                stall_after_progress(&mut writer, &peer).await;
                transport.close(&context).unwrap();
                let reader = async {
                    let mut received = Vec::new();
                    peer.read_to_end(&mut received).await.unwrap();
                    received
                };
                let (receipt, received) =
                    tokio::join!(poll_fn(|context| writer.poll_complete(context)), reader);
                assert_eq!(receipt.result.unwrap().bytes_written, STALLING_WRITE);
                assert!(
                    received == payload,
                    "the peer reads the whole write, then the FIN"
                );
            });
        }

        #[test]
        fn cancellation_fails_the_outstanding_write_and_resets_the_transport() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let cancellation = CancellationSource::new();
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        cancellation.token(),
                    )
                    .unwrap()
                    .unwrap();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 11)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                cancellation.request_cancellation();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                let error = receipt.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert!(error.bytes_written() > 0);
                assert!(
                    writer.is_saturated(),
                    "the cancelled write reset the transport"
                );
                let mut drained = Vec::new();
                assert_eq!(
                    peer.read_to_end(&mut drained).await.unwrap_err().kind(),
                    io::ErrorKind::ConnectionReset,
                );
            });
        }

        #[test]
        fn a_cancellation_reset_whose_wake_panics_keeps_the_receipt_for_the_next_poll() {
            struct PanickingWake;
            impl Wake for PanickingWake {
                fn wake(self: Arc<Self>) {
                    panic!("the reset's wake panics");
                }
            }
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, peer| async move {
                let cancellation = CancellationSource::new();
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        cancellation.token(),
                    )
                    .unwrap()
                    .unwrap();
                // A write larger than the socket buffers stays outstanding past its first attempt.
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 31)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                cancellation.request_cancellation();
                let waker = Waker::from(Arc::new(PanickingWake));
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    writer.poll_complete(&mut Context::from_waker(&waker))
                }))
                .expect_err("the cancellation's reset wakes the polling task, which panics");
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"the reset's wake panics")
                );
                let capacity = writer.can_write(1);
                let polled = writer.poll_complete(&mut Context::from_waker(Waker::noop()));
                assert_eq!(
                    (capacity, polled.is_ready()),
                    (false, true),
                    "the reset write holds the queue's slot until the next poll delivers its receipt",
                );
                let Poll::Ready(receipt) = polled else {
                    unreachable!("the receipt was delivered");
                };
                assert_eq!(receipt.requested_bytes, STALLING_WRITE);
                assert_eq!(
                    receipt.result.unwrap_err().code(),
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    "the next poll finds the transport reset",
                );
                assert!(writer.is_idle() && writer.is_saturated());
            });
        }

        #[test]
        fn dropping_the_queue_with_an_outstanding_write_resets_the_transport() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 13)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                drop(writer);
                assert!(
                    !transport.has_pending_write(&context).unwrap(),
                    "the abandoned write is retired"
                );
                let error = transport
                    .write(
                        &context,
                        queue(&[b"x"]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.code(),
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    "the transport was reset, as for a dropped write future"
                );
                let mut drained = Vec::new();
                assert_eq!(
                    peer.read_to_end(&mut drained).await.unwrap_err().kind(),
                    io::ErrorKind::ConnectionReset,
                );
            });
        }

        #[test]
        fn submissions_without_capacity_or_over_the_bound_are_misuse() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 8, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let error = writer.start_write(queue(&[b"012345678"])).unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert!(writer.is_idle(), "a refused submission leaves no receipt");
                writer.start_write(queue(&[b"01234567"])).unwrap();
                assert!(!writer.can_write(1));
                let error = writer.start_write(queue(&[b"8"])).unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.result.unwrap().bytes_written, 8);
                assert!(writer.is_idle(), "the refused submission left no receipt");
                let mut received = [0; 8];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"01234567");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_stream_without_native_queue_support_offers_no_queue() {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(|context| async move {
                            let transport = owned_from_stream(
                                &context,
                                DrainStream {
                                    control: DrainControl::default(),
                                    worker: context.handle().clone(),
                                },
                                SocketAddress::new("127.0.0.1", 1000),
                                SocketAddress::new("127.0.0.1", 2000),
                                None,
                                limits(),
                            )
                            .unwrap();
                            assert!(
                                transport
                                    .write_queue(
                                        &context,
                                        1024,
                                        Duration::ZERO,
                                        CancellationToken::never(),
                                    )
                                    .unwrap()
                                    .is_none(),
                                "only TCP streams drive queued writes; others keep the \
                                 single-write API",
                            );
                            transport.close_with_reset(&context).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                }));
        }

        #[test]
        fn a_pending_queued_write_arms_no_deadline_baton() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                let batons = write_deadline_batons_for_test();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 17)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                assert_eq!(
                    write_deadline_batons_for_test(),
                    batons,
                    "an untimed queued write that pends arms no per-write deadline"
                );
                let drain = async {
                    let mut received = vec![0; STALLING_WRITE];
                    peer.read_exact(&mut received).await.unwrap();
                };
                let (receipt, ()) =
                    tokio::join!(poll_fn(|context| writer.poll_complete(context)), drain);
                receipt.result.unwrap();
                drop(writer);

                // Control: the single-write API arms one deadline for a write that pends.
                let mut write = transport.write(
                    &context,
                    queue(&[&pattern(STALLING_WRITE, 19)]),
                    Duration::ZERO,
                    CancellationToken::never(),
                );
                let mut byte = [0];
                tokio::select! {
                    _ = &mut write => panic!("the write completes only as the peer reads"),
                    peeked = peer.peek(&mut byte) => assert_eq!(peeked.unwrap(), 1),
                }
                assert_eq!(write_deadline_batons_for_test(), batons + 1);
                let drain = async {
                    let mut received = vec![0; STALLING_WRITE];
                    peer.read_exact(&mut received).await.unwrap();
                };
                let (written, ()) = tokio::join!(write, drain);
                written.unwrap();
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn queued_writes_reuse_one_progress_cell() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let cells = write_progress_cells_for_test();
                for chunk in [&b"one "[..], b"two "] {
                    writer.start_write(queue(&[chunk])).unwrap();
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(
                        receipt.result.unwrap().bytes_written,
                        4,
                        "each write counts its own progress"
                    );
                }
                assert_eq!(
                    write_progress_cells_for_test(),
                    cells,
                    "queued writes count their progress in the queue's one cell"
                );
                // Control: the single-write API builds one cell per write.
                transport
                    .write(
                        &context,
                        queue(&[b"six "]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(write_progress_cells_for_test(), cells + 1);
                let mut received = [0; 12];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"one two six ");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn queued_writes_build_no_io_write_future() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let futures = io_write_futures_for_test();
                for chunk in [&b"one "[..], b"two "] {
                    writer.start_write(queue(&[chunk])).unwrap();
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(receipt.result.unwrap().bytes_written, 4);
                }
                assert_eq!(
                    io_write_futures_for_test(),
                    futures,
                    "queued writes keep their I/O state in the backend, not in a boxed future"
                );
                // Control: the single-write API builds one I/O future per write.
                transport
                    .write(
                        &context,
                        queue(&[b"six "]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(io_write_futures_for_test(), futures + 1);
                let mut received = [0; 12];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"one two six ");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn queued_writes_reuse_the_queues_operation_slot() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let operations = write_operations_for_test();
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                for chunk in [&b"one "[..], b"two "] {
                    writer.start_write(queue(&[chunk])).unwrap();
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(receipt.result.unwrap().bytes_written, 4);
                }
                assert_eq!(
                    write_operations_for_test(),
                    operations + 1,
                    "a queue builds one write operation, its slot, and its writes reuse it"
                );
                drop(writer);
                // Control: the single-write API builds one write operation per write.
                transport
                    .write(
                        &context,
                        queue(&[b"six "]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(write_operations_for_test(), operations + 2);
                let mut received = [0; 12];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"one two six ");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn dropping_an_idle_queue_leaves_the_transport_open() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                writer.start_write(queue(&[b"one "])).unwrap();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.result.unwrap().bytes_written, 4);
                drop(writer);
                drop(
                    transport
                        .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap(),
                );
                let written = transport
                    .write(
                        &context,
                        queue(&[b"two "]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    written.bytes_written, 4,
                    "dropping a queue with no outstanding write leaves the transport writable"
                );
                let mut received = [0; 8];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"one two ");
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_cancelled_queue_refuses_its_submission_and_resets_the_transport() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let cancellation = CancellationSource::new();
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, cancellation.token())
                    .unwrap()
                    .unwrap();
                cancellation.request_cancellation();
                writer.start_write(queue(&[b"late"])).unwrap();
                assert!(
                    !writer.is_idle() && writer.can_write(1),
                    "the refused submission holds its receipt but no write"
                );
                let receipt = poll_once(&mut writer).await;
                let Poll::Ready(receipt) = receipt else {
                    panic!("a submission refused at entry has its receipt at once");
                };
                assert_eq!(receipt.requested_bytes, 4);
                let error = receipt.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(error.bytes_written(), 0);
                assert!(
                    writer.is_saturated(),
                    "the cancellation reset the transport"
                );
                let mut drained = Vec::new();
                assert_eq!(
                    peer.read_to_end(&mut drained).await.unwrap_err().kind(),
                    io::ErrorKind::ConnectionReset,
                );
            });
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn queued_writes_build_no_heap_iovec_array() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let arrays = send_iovec_arrays_for_test();
                for segments in [
                    [&b"o"[..], &b"n"[..], &b"e "[..]],
                    [&b"t"[..], &b"w"[..], &b"o "[..]],
                ] {
                    writer.start_write(queue(&segments)).unwrap();
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(receipt.result.unwrap().bytes_written, 4);
                }
                assert_eq!(
                    send_iovec_arrays_for_test(),
                    arrays,
                    "queued writes of a few segments pass their iovecs from the stack"
                );
                // Control: a write of more than 64 segments builds one heap iovec array, as
                // `folly::AsyncSocket::writeChain` allocates beyond `kSmallIoVecSize`.
                let many = [&b"x"[..]; 65];
                writer.start_write(queue(&many)).unwrap();
                let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                assert_eq!(receipt.result.unwrap().bytes_written, 65);
                assert_eq!(send_iovec_arrays_for_test(), arrays + 1);
                let mut received = [0; 73];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received[..8], b"one two ");
                assert_eq!(&received[8..], [b'x'; 65]);
                transport.close(&context).unwrap();
            });
        }

        /// A reset of the transport wakes the task that polled the outstanding write, so that
        /// task observes the failure receipt without polling again for another reason.
        #[test]
        fn a_reset_wakes_the_task_awaiting_the_outstanding_write() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 17)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                let wake = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&wake));
                assert!(
                    writer
                        .poll_complete(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert!(!wake.take(), "the stalled write has not woken its task");
                transport.close_with_reset(&context).unwrap();
                assert!(wake.take(), "the reset wakes the task awaiting the write");
                let Poll::Ready(receipt) = poll_once(&mut writer).await else {
                    panic!("the reset completes the outstanding write");
                };
                assert_eq!(
                    receipt.result.unwrap_err().code(),
                    HttpErrorCode::TRANSPORT_WRITE_ERROR
                );
            });
        }

        /// A cancellation requested from another thread wakes the task that last polled the
        /// outstanding write, which then observes the cancelled receipt and the reset transport.
        #[test]
        fn a_cancellation_from_another_thread_wakes_the_task_awaiting_the_outstanding_write() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let cancellation = CancellationSource::new();
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        cancellation.token(),
                    )
                    .unwrap()
                    .unwrap();
                writer
                    .start_write(queue(&[&pattern(STALLING_WRITE, 19)]))
                    .unwrap();
                stall_after_progress(&mut writer, &peer).await;
                let wake = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&wake));
                assert!(
                    writer
                        .poll_complete(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert!(!wake.take(), "the stalled write has not woken its task");
                std::thread::spawn(move || cancellation.request_cancellation())
                    .join()
                    .unwrap();
                time::timeout(Duration::from_secs(2), wake.wait())
                    .await
                    .expect("the cancellation wakes the task awaiting the write");
                let Poll::Ready(receipt) = poll_once(&mut writer).await else {
                    panic!("the cancellation completes the outstanding write");
                };
                assert_eq!(
                    receipt.result.unwrap_err().code(),
                    HttpErrorCode::CORO_CANCELLED
                );
                assert!(
                    writer.is_saturated(),
                    "the cancelled write reset the transport"
                );
                let mut drained = Vec::new();
                assert_eq!(
                    peer.read_to_end(&mut drained).await.unwrap_err().kind(),
                    io::ErrorKind::ConnectionReset,
                );
            });
        }

        /// Lets the reactor observe a new socket's writability, as it has by the time a
        /// connection that has read anything writes. Until then a send waits for readiness
        /// without attempting the socket.
        async fn observe_writability() {
            tokio::task::yield_now().await;
        }

        /// Toward `folly::AsyncSocket::writeImpl`, which writes at once when no write is
        /// pending: a submission to an idle queue goes out at entry when its first attempt
        /// finishes it. Its capacity and saturation still wait for the next poll, which delivers
        /// its receipt, as when that poll wrote it.
        #[test]
        fn a_submission_its_first_attempt_finishes_is_written_at_entry() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                observe_writability().await;
                writer.start_write(queue(&[b"thr", b"ough"])).unwrap();
                let mut received = [0; 7];
                time::timeout(Duration::from_secs(2), peer.read_exact(&mut received))
                    .await
                    .expect("the peer receives the submission before the queue is polled")
                    .unwrap();
                assert_eq!(&received, b"through");
                assert!(
                    !transport.has_pending_write(&context).unwrap(),
                    "a write finished at entry leaves no transport write pending"
                );
                assert!(
                    !writer.can_write(1) && writer.is_saturated() && !writer.is_idle(),
                    "capacity returns at the poll that delivers the receipt"
                );
                let Poll::Ready(receipt) = poll_once(&mut writer).await else {
                    panic!("the next poll delivers the finished write's receipt");
                };
                assert_eq!(receipt.requested_bytes, 7);
                assert_eq!(receipt.result.unwrap().bytes_written, 7);
                assert!(writer.can_write(1) && !writer.is_saturated() && writer.is_idle());
                transport.close(&context).unwrap();
            });
        }

        /// A write that its first attempt finishes is never registered with the transport as
        /// an operation.
        #[test]
        fn writes_finished_at_entry_register_no_transport_write() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                observe_writability().await;
                let registrations = write_registrations_for_test();
                for chunk in [&b"one "[..], b"two "] {
                    writer.start_write(queue(&[chunk])).unwrap();
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(receipt.result.unwrap().bytes_written, 4);
                }
                assert_eq!(
                    write_registrations_for_test(),
                    registrations,
                    "writes finished at entry register no transport write"
                );
                drop(writer);
                // Control: the single-write API registers each write.
                transport
                    .write(
                        &context,
                        queue(&[b"six "]),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(write_registrations_for_test(), registrations + 1);
                let mut received = [0; 12];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"one two six ");
                transport.close(&context).unwrap();
            });
        }

        /// A first attempt that the socket takes only part of leaves the rest of the write
        /// outstanding, registered once, and the queue's polls finish it in order.
        #[test]
        fn a_partial_first_attempt_leaves_the_rest_outstanding() {
            let limits = ByteStreamLimits::new(64, STALLING_WRITE, 8).unwrap();
            with_accepted_tcp(limits, |context, transport, mut peer| async move {
                let mut writer = transport
                    .write_queue(
                        &context,
                        STALLING_WRITE,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .unwrap()
                    .unwrap();
                let registrations = write_registrations_for_test();
                let payload = pattern(STALLING_WRITE, 23);
                observe_writability().await;
                writer.start_write(queue(&[&payload])).unwrap();
                let mut byte = [0];
                let peeked = time::timeout(Duration::from_secs(2), peer.peek(&mut byte))
                    .await
                    .expect("the first attempt sends what the socket takes before any poll")
                    .unwrap();
                assert_eq!(peeked, 1);
                assert!(
                    transport.has_pending_write(&context).unwrap(),
                    "the rest of the write is outstanding"
                );
                assert!(writer.is_saturated() && !writer.can_write(1) && !writer.is_idle());
                assert_eq!(
                    write_registrations_for_test(),
                    registrations + 1,
                    "the unfinished write is registered once"
                );
                assert!(poll_once(&mut writer).await.is_pending());
                let reader = async {
                    let mut received = vec![0; STALLING_WRITE];
                    peer.read_exact(&mut received).await.unwrap();
                    received
                };
                let (receipt, received) =
                    tokio::join!(poll_fn(|context| writer.poll_complete(context)), reader);
                assert_eq!(receipt.requested_bytes, STALLING_WRITE);
                assert_eq!(receipt.result.unwrap().bytes_written, STALLING_WRITE);
                assert!(
                    received == payload,
                    "the peer receives the payload in order"
                );
                assert_eq!(write_registrations_for_test(), registrations + 1);
                assert!(!writer.is_saturated() && writer.can_write(1) && writer.is_idle());
                transport.close(&context).unwrap();
            });
        }

        /// A first attempt that finds the socket full (`WouldBlock`) leaves the whole write
        /// outstanding. The poll that pends registers the polling task's waker, so the
        /// socket's writability wakes that task and the next poll finishes the write.
        #[test]
        fn a_first_attempt_that_would_block_leaves_the_write_to_the_queues_polls() {
            with_observed_tcp(
                limits(),
                |context, transport, mut peer, mut observer| async move {
                    let mut writer = transport
                        .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap();
                    // Fill the socket behind the transport once the reactor has seen it
                    // writable, so that the first attempt's send would block.
                    observe_writability().await;
                    observer.set_nonblocking(true).unwrap();
                    let filler = pattern(64 << 10, 29);
                    let mut filled = 0;
                    loop {
                        match io::Write::write(&mut observer, &filler) {
                            Ok(written) => filled += written,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                            Err(error) => panic!("filling the socket failed: {error}"),
                        }
                    }
                    writer.start_write(queue(&[b"after"])).unwrap();
                    assert!(
                        transport.has_pending_write(&context).unwrap(),
                        "a blocked first attempt leaves the write outstanding"
                    );
                    assert!(writer.is_saturated() && !writer.can_write(1));
                    let wake = Arc::new(WakeSignal::default());
                    let waker = Waker::from(Arc::clone(&wake));
                    assert!(
                        writer
                            .poll_complete(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    let mut received = vec![0; filled];
                    peer.read_exact(&mut received).await.unwrap();
                    time::timeout(Duration::from_secs(5), wake.wait())
                        .await
                        .expect("the socket's writability wakes the task that polled the write");
                    let receipt = poll_fn(|context| writer.poll_complete(context)).await;
                    assert_eq!(receipt.requested_bytes, 5);
                    assert_eq!(receipt.result.unwrap().bytes_written, 5);
                    let mut after = [0; 5];
                    peer.read_exact(&mut after).await.unwrap();
                    assert_eq!(&after, b"after");
                    assert!(writer.can_write(1) && writer.is_idle());
                    transport.close(&context).unwrap();
                },
            );
        }

        /// A first attempt that fails delivers its failure receipt, with the bytes it wrote, at
        /// the next poll, as a first poll's failure did; capacity waits for that poll.
        #[test]
        fn a_first_attempt_that_fails_delivers_its_receipt_at_the_next_poll() {
            with_observed_tcp(limits(), |context, transport, peer, observer| async move {
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                #[expect(deprecated, reason = "Tokio's only linger-zero reset setter")]
                let linger = peer.set_linger(Some(Duration::ZERO));
                linger.unwrap();
                drop(peer);
                // The peer's reset has arrived once the socket reports it.
                observer.set_nonblocking(true).unwrap();
                while observer
                    .peek(&mut [0])
                    .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
                {
                    time::sleep(Duration::from_millis(1)).await;
                }
                observe_writability().await;
                writer.start_write(queue(&[b"lost"])).unwrap();
                assert!(!writer.can_write(1) && writer.is_saturated() && !writer.is_idle());
                let Poll::Ready(receipt) = poll_once(&mut writer).await else {
                    panic!("the next poll delivers the failed write's receipt");
                };
                assert_eq!(receipt.requested_bytes, 4);
                let error = receipt.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(error.bytes_written(), 0);
                assert!(writer.can_write(1) && writer.is_idle());
                transport.close_with_reset(&context).unwrap();
            });
        }

        /// The queue's cancellation applies to its outstanding write. A write that its first
        /// attempt finished is no longer outstanding, so a later cancellation leaves its receipt
        /// and refuses the next submission at entry, as for an idle queue.
        #[test]
        fn a_cancellation_after_a_write_finished_at_entry_refuses_the_next_submission() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let cancellation = CancellationSource::new();
                let mut writer = transport
                    .write_queue(&context, 1024, Duration::ZERO, cancellation.token())
                    .unwrap()
                    .unwrap();
                observe_writability().await;
                writer.start_write(queue(&[b"done"])).unwrap();
                cancellation.request_cancellation();
                let Poll::Ready(receipt) = poll_once(&mut writer).await else {
                    panic!("the next poll delivers the finished write's receipt");
                };
                assert_eq!(receipt.result.unwrap().bytes_written, 4);
                assert!(
                    !writer.is_saturated(),
                    "the transport stays open until a submission observes the cancellation"
                );
                writer.start_write(queue(&[b"late"])).unwrap();
                let Poll::Ready(late) = poll_once(&mut writer).await else {
                    panic!("a submission refused at entry has its receipt at once");
                };
                let error = late.result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(error.bytes_written(), 0);
                assert!(writer.is_saturated(), "the refusal reset the transport");
                let mut received = [0; 4];
                peer.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"done");
                let mut drained = Vec::new();
                assert_eq!(
                    peer.read_to_end(&mut drained).await.unwrap_err().kind(),
                    io::ErrorKind::ConnectionReset,
                );
            });
        }

        /// A first attempt that panics leaves the write outstanding, as a first poll that
        /// panicked did: the panic surfaces from `start_write`, the write stays registered and
        /// holds the queue's capacity, and the next poll continues it.
        #[test]
        fn a_first_attempt_that_panics_leaves_the_write_outstanding() {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(|context| async move {
                            let written = Rc::new(RefCell::new(Vec::new()));
                            let transport = owned_from_stream(
                                &context,
                                FirstWritePanicStream {
                                    panicked: false,
                                    written: Rc::clone(&written),
                                },
                                SocketAddress::new("127.0.0.1", 1000),
                                SocketAddress::new("127.0.0.1", 2000),
                                None,
                                limits(),
                            )
                            .unwrap();
                            let mut writer = transport
                                .write_queue(
                                    &context,
                                    1024,
                                    Duration::ZERO,
                                    CancellationToken::never(),
                                )
                                .unwrap()
                                .expect("the stream offers the native write queue");
                            let panic = catch_unwind(AssertUnwindSafe(|| {
                                writer.start_write(queue(&[b"data"]))
                            }))
                            .expect_err("the first attempt's panic surfaces from the submission");
                            assert_eq!(
                                panic.downcast_ref::<&str>(),
                                Some(&"the first write attempt panics")
                            );
                            assert!(
                                transport.has_pending_write(&context).unwrap(),
                                "the panicked write stays registered"
                            );
                            assert!(
                                !writer.can_write(1) && writer.is_saturated() && !writer.is_idle()
                            );
                            let Poll::Ready(receipt) =
                                writer.poll_complete(&mut Context::from_waker(Waker::noop()))
                            else {
                                panic!("the next poll continues the write to its end");
                            };
                            assert_eq!(receipt.requested_bytes, 4);
                            assert_eq!(receipt.result.unwrap().bytes_written, 4);
                            assert_eq!(&*written.borrow(), b"data");
                            assert!(writer.can_write(1) && writer.is_idle());
                            transport.close_with_reset(&context).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                }));
        }
    }

    mod owned_reader {
        use super::*;
        use crate::tcp::io_read_futures_for_test;
        use crate::tcp_tls_tests::tcp_tls::native_write_queue::with_accepted_tcp;

        /// As `with_accepted_tcp`, with a peer that disables Nagle's algorithm and a clone of the
        /// accepted socket through which the test sees how many bytes are queued.
        pub(super) fn with_observed_tcp<F, Fut>(limits: ByteStreamLimits, test: F)
        where
            F: FnOnce(WorkerContext, OwnedByteStream, TcpStream, std::net::TcpStream) -> Fut
                + Send
                + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    peer.set_nodelay(true).unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let accepted = accepted.into_std().unwrap();
                    let observer = accepted.try_clone().unwrap();
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport =
                                TcpTransport::from_accepted(&context, accepted, limits).unwrap();
                            test(context, transport, peer, observer).await;
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(10), call)
                        .await
                        .expect("the reader test is bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                }));
        }

        /// `DIV-018`: once a reader read's bytes are released, the next read reclaims the same
        /// buffer, so sequential reads make one first-buffer reservation, where a buffer per read
        /// would make three.
        #[test]
        fn tcp_reader_reads_reuse_one_reclaimed_buffer() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let reservations = tcp_first_reservations_for_test();
                let mut start = None;
                for chunk in [&b"one "[..], b"two ", b"six "] {
                    peer.write_all(chunk).await.unwrap();
                    reader.start_read(64, 64).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    let front = read.data().front().unwrap().as_ptr() as usize;
                    assert_eq!(
                        *start.get_or_insert(front),
                        front,
                        "each read fills the reclaimed buffer from its start",
                    );
                    assert_eq!(read_bytes(read), chunk);
                }
                assert_eq!(
                    tcp_first_reservations_for_test(),
                    reservations + 1,
                    "the reads reuse the reader's one buffer",
                );
                transport.close(&context).unwrap();
            });
        }

        /// `DIV-018`, toward `IOBufQueue::preallocate`: while earlier reads' bytes are held, a
        /// reader read fills the rest of their buffer as long as at least Proxygen's
        /// `kMinReadSize` (1_460) bytes of it are left, and otherwise takes a new
        /// `allocation_size` buffer. Each read's view reports its whole buffer as backing.
        #[test]
        fn tcp_reader_reads_fill_a_retained_buffers_tailroom() {
            let limits = ByteStreamLimits::new(8_000, 1024, 8).unwrap();
            with_observed_tcp(
                limits,
                |context, transport, mut peer, observer| async move {
                    let mut reader = transport
                        .reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap();
                    let reservations = tcp_first_reservations_for_test();
                    // 2_000 and 540 bytes leave 1_460 bytes of the 4_000-byte buffer, which the third
                    // read still fills; its 100 bytes leave 1_360, so the fourth takes a new buffer.
                    let mut held = Vec::new();
                    for (seed, length) in (0u8..).zip([2_000, 540, 100, 100]) {
                        let burst = pattern(length, seed);
                        peer.write_all(&burst).await.unwrap();
                        wait_until_queued(&observer, length).await;
                        reader.start_read(8_000, 4_000).unwrap();
                        let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                        assert_eq!(read.data().segment_count(), 1, "{length}");
                        assert_eq!(
                            read.data().backing_capacity_upper_bound(),
                            Some(4_000),
                            "a {length}-byte read's view pins its whole buffer",
                        );
                        assert!(read_bytes_ref(&read) == burst, "{length}");
                        held.push(read);
                    }
                    let starts: Vec<usize> = held
                        .iter()
                        .map(|read| read.data().front().unwrap().as_ptr() as usize)
                        .collect();
                    assert_eq!(
                        starts[1],
                        starts[0] + 2_000,
                        "the second read fills the held buffer's tailroom",
                    );
                    assert_eq!(
                        starts[2],
                        starts[0] + 2_540,
                        "1_460 bytes of tailroom are still filled",
                    );
                    assert!(
                        !(starts[0]..starts[0] + 4_000).contains(&starts[3]),
                        "1_360 bytes of tailroom are not",
                    );
                    assert_eq!(tcp_first_reservations_for_test(), reservations + 2);
                    transport.close(&context).unwrap();
                },
            );
        }

        /// A first offer of partial tailroom that the read fills continues the read, as a full
        /// first buffer does (`DIV-017`), so one read still returns the whole burst.
        #[test]
        fn a_tcp_reader_read_that_fills_partial_tailroom_continues() {
            let limits = ByteStreamLimits::new(8_000, 1024, 8).unwrap();
            with_observed_tcp(
                limits,
                |context, transport, mut peer, observer| async move {
                    let mut reader = transport
                        .reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap();
                    let first = pattern(2_000, 1);
                    peer.write_all(&first).await.unwrap();
                    wait_until_queued(&observer, first.len()).await;
                    reader.start_read(8_000, 4_000).unwrap();
                    let held = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert!(read_bytes_ref(&held) == first);
                    // `held` keeps the buffer, so 2_000 bytes of its tailroom are offered first.
                    let burst = pattern(5_000, 2);
                    peer.write_all(&burst).await.unwrap();
                    wait_until_queued(&observer, burst.len()).await;
                    reader.start_read(8_000, 4_000).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert!(
                        read_bytes_ref(&read) == burst,
                        "one read returns the whole 5_000-byte burst",
                    );
                    drop(held);
                    transport.close(&context).unwrap();
                },
            );
        }

        /// A reader read that finds no data reserves no buffer; the first bytes reserve one.
        #[test]
        fn a_pending_tcp_reader_read_reserves_nothing_before_data() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let reservations = tcp_first_reservations_for_test();
                reader.start_read(64, 64).unwrap();
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert_eq!(
                    tcp_first_reservations_for_test(),
                    reservations,
                    "a reader read that finds no data reserves no buffer",
                );
                peer.write_all(b"data").await.unwrap();
                let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(read_bytes(read), b"data");
                assert_eq!(tcp_first_reservations_for_test(), reservations + 1);
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn tcp_reader_reads_build_no_io_read_future() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let futures = io_read_futures_for_test();
                for chunk in [&b"one "[..], b"two "] {
                    peer.write_all(chunk).await.unwrap();
                    reader.start_read(64, 64).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert_eq!(read_bytes(read), chunk);
                }
                assert_eq!(
                    io_read_futures_for_test(),
                    futures,
                    "reader reads keep their I/O state in the backend, not in a boxed future",
                );
                // Control: the read API builds one I/O future per read.
                peer.write_all(b"six ").await.unwrap();
                let read = transport
                    .read(&context, 64, 64, Duration::ZERO, CancellationToken::never())
                    .await
                    .unwrap();
                assert_eq!(read_bytes(read), b"six ");
                assert_eq!(io_read_futures_for_test(), futures + 1);
                transport.close(&context).unwrap();
            });
        }

        /// A reader read that ends unfinished, dropped or cancelled, releases its backend state,
        /// so the next read starts clean.
        #[test]
        fn an_unfinished_tcp_reader_read_releases_its_backend_state() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                for cancel in [false, true] {
                    let source = CancellationSource::new();
                    let mut reader = transport
                        .reader(&context, Duration::ZERO, source.token())
                        .unwrap()
                        .unwrap();
                    reader.start_read(64, 64).unwrap();
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    if cancel {
                        source.request_cancellation();
                        assert_eq!(
                            poll_fn(|context| reader.poll_read(context))
                                .await
                                .unwrap_err()
                                .code(),
                            HttpErrorCode::CORO_CANCELLED,
                        );
                    }
                    drop(reader);
                    let mut reader = transport
                        .reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap();
                    peer.write_all(b"next").await.unwrap();
                    reader.start_read(64, 64).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert_eq!(read_bytes(read), b"next", "cancelled: {cancel}");
                }
                transport.close(&context).unwrap();
            });
        }

        /// `DIV-016` through a reader: the bytes queued before a TCP reset come first although
        /// the read's continuation poll met the reset, the next read reports the reset once,
        /// and the one after is EOF.
        #[cfg(target_os = "linux")]
        #[test]
        fn tcp_reader_returns_bytes_before_a_later_reset() {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let accepted = accepted.into_std().unwrap();
                    let observer = accepted.try_clone().unwrap();
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport = TcpTransport::from_accepted(
                                &context,
                                accepted,
                                ByteStreamLimits::new(8_000, 1024, 8).unwrap(),
                            )
                            .unwrap();
                            let burst = pattern(1_000, 9);
                            let mut peer = peer;
                            peer.write_all(&burst).await.unwrap();
                            #[expect(deprecated, reason = "Tokio's only linger-zero reset setter")]
                            let linger = peer.set_linger(Some(Duration::ZERO));
                            linger.unwrap();
                            drop(peer);
                            TcpStream::from_std(observer)
                                .unwrap()
                                .ready(Interest::ERROR)
                                .await
                                .unwrap();
                            let mut reader = transport
                                .reader(&context, Duration::ZERO, CancellationToken::never())
                                .unwrap()
                                .unwrap();
                            reader.start_read(8_000, 1_000).unwrap();
                            let first = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                            assert!(!first.is_eof());
                            assert!(
                                read_bytes(first) == burst,
                                "the bytes queued before the reset come first",
                            );
                            reader.start_read(8_000, 1_000).unwrap();
                            let error = poll_fn(|context| reader.poll_read(context))
                                .await
                                .unwrap_err();
                            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                            let cause =
                                error.source().unwrap().downcast_ref::<io::Error>().unwrap();
                            assert_eq!(cause.kind(), io::ErrorKind::ConnectionReset);
                            reader.start_read(8_000, 1_000).unwrap();
                            assert!(
                                poll_fn(|context| reader.poll_read(context))
                                    .await
                                    .unwrap()
                                    .is_eof(),
                                "the socket reports the reset only once",
                            );
                            transport.close_with_reset(&context).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("reader reads after a TCP reset remain bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                }));
        }

        /// Reader reads are bounded by the maximum, keep every byte, and end at EOF, as
        /// `read`'s do.
        #[test]
        fn tcp_reader_reads_keep_their_sizes_bytes_and_eof() {
            // As `allocation_sized_reads_preserve_bytes_and_eof`: a 7-byte allocation size and
            // a 64-byte maximum read a 1_000-byte payload in fifteen 64-byte reads and a 40-byte
            // one.
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(LocalSet::new().run_until(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let mut peer = TcpStream::connect(listener.local_addr().unwrap())
                        .await
                        .unwrap();
                    let (accepted, _) = listener.accept().await.unwrap();
                    let payload = pattern(1_000, 3);
                    peer.write_all(&payload).await.unwrap();
                    peer.shutdown().await.unwrap();
                    let mut queued = vec![0; payload.len()];
                    while accepted.peek(&mut queued).await.unwrap() < payload.len() {
                        tokio::task::yield_now().await;
                    }
                    let accepted = accepted.into_std().unwrap();
                    let (worker, driver) = WorkerDriver::new(8).unwrap();
                    let call = worker
                        .call_local(move |context| async move {
                            let transport =
                                TcpTransport::from_accepted(&context, accepted, limits()).unwrap();
                            let mut reader = transport
                                .reader(&context, Duration::ZERO, CancellationToken::never())
                                .unwrap()
                                .unwrap();
                            let mut received = Vec::new();
                            let mut sizes = Vec::new();
                            loop {
                                reader.start_read(64, 7).unwrap();
                                let read =
                                    poll_fn(|context| reader.poll_read(context)).await.unwrap();
                                if read.is_eof() {
                                    assert_eq!(read.bytes_read(), 0, "EOF follows the last bytes");
                                    break;
                                }
                                sizes.push(read.bytes_read());
                                received.extend(read_bytes(read));
                            }
                            assert!(received == payload);
                            assert_eq!(sizes, [vec![64; 15], vec![40]].concat());
                            transport.close(&context).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    time::timeout(Duration::from_secs(5), call)
                        .await
                        .expect("TCP reader reads to EOF remain bounded")
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                    drop(peer);
                }));
        }

        /// `DIV-018` through a reader: a TCP reader read's first buffer has the allocation size's
        /// capacity, as `read`'s does.
        #[test]
        fn tcp_reader_read_first_buffer_has_the_allocation_size() {
            with_accepted_tcp(limits(), |context, transport, mut peer| async move {
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                peer.write_all(b"abc").await.unwrap();
                reader.start_read(64, 7).unwrap();
                let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(
                    read.data().backing_capacity_upper_bound(),
                    Some(7),
                    "the 3 bytes arrive in the 7-byte first buffer",
                );
                assert_eq!(read_bytes(read), b"abc");
                transport.close(&context).unwrap();
            });
        }
    }
}
