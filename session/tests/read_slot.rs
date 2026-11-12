//! `ReadSlot` outcomes, pinned for the same scripted reads through each read path: one
//! `UniplexTransport::read` future per read, and the transport's `UniplexReader`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use super::ReadSlot;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexReader;
use crate::uniplex::UniplexTransport;
use crate::uniplex_tests::run_on_worker;

/// What one poll of a scripted read returns.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// Pends.
    Pending,
    /// Returns the bytes; with `true`, the reader then reports itself drained.
    Data(&'static str, bool),
    Eof,
    Fail(HttpErrorCode),
    /// Fails with `CORO_CANCELLED` once the read's cancellation is requested, else pends.
    Cancellable,
    /// Refuses the read as it starts: the reader's `start_read` fails, and the future fails
    /// at its first poll.
    Refuse(HttpErrorCode),
    Panic,
}

/// The reads both paths of one case serve, and what they observe.
struct Script {
    steps: RefCell<VecDeque<Step>>,
    log: RefCell<Vec<String>>,
    drained: Cell<bool>,
    /// The next read's start (`read` or `start_read`) panics.
    panic_on_start: Cell<bool>,
    /// The next read future's drop panics.
    panic_on_drop: Cell<bool>,
    /// Each read future's drop wakes the task's waker.
    wake_on_drop: bool,
    waker: Waker,
}

impl Script {
    fn note(&self, entry: String) {
        self.log.borrow_mut().push(entry);
    }

    /// Pops the next step and returns its outcome, as a poll of the started read.
    fn poll(
        &self,
        context: &Context<'_>,
        cancellation: &CancellationToken,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        let step = self
            .steps
            .borrow_mut()
            .pop_front()
            .expect("a scripted step");
        let waker = if context.waker().will_wake(&self.waker) {
            "slot's"
        } else {
            "other"
        };
        self.note(format!("  poll {step:?} waker={waker}"));
        match step {
            Step::Pending => Poll::Pending,
            Step::Data(bytes, drained) => {
                self.drained.set(drained);
                Poll::Ready(Ok(UniplexRead::new(
                    BufQueue::from_vec(bytes.as_bytes().to_vec(), usize::MAX).expect("unbounded"),
                    false,
                )))
            }
            Step::Eof => {
                self.drained.set(false);
                Poll::Ready(Ok(UniplexRead::new(BufQueue::new(0), true)))
            }
            Step::Fail(code) | Step::Refuse(code) => {
                Poll::Ready(Err(HttpError::with_message(code, b"scripted failure")))
            }
            Step::Cancellable if cancellation.is_cancellation_requested() => {
                Poll::Ready(Err(HttpError::new(HttpErrorCode::CORO_CANCELLED)))
            }
            Step::Cancellable => Poll::Pending,
            Step::Panic => panic!("scripted poll panic"),
        }
    }

    fn start(&self) {
        if self.panic_on_start.take() {
            panic!("scripted start panic");
        }
    }
}

struct ScriptRead {
    script: Rc<Script>,
    cancellation: CancellationToken,
}

impl Future for ScriptRead {
    type Output = Result<UniplexRead, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.script.poll(context, &self.cancellation)
    }
}

impl Drop for ScriptRead {
    fn drop(&mut self) {
        self.script.note("  drop read future".to_owned());
        if self.script.wake_on_drop {
            self.script.note("  wake".to_owned());
            self.script.waker.wake_by_ref();
        }
        if self.script.panic_on_drop.take() {
            panic!("scripted drop panic");
        }
    }
}

struct ScriptReader {
    script: Rc<Script>,
    cancellation: CancellationToken,
}

impl UniplexReader for ScriptReader {
    fn start_read(&mut self, maximum: usize, allocation_size: usize) -> Result<(), HttpError> {
        self.script.note(format!(
            "  start_read maximum={maximum} allocation={allocation_size}"
        ));
        self.script.start();
        let refused = match self.script.steps.borrow().front() {
            Some(Step::Refuse(code)) => Some(*code),
            _ => None,
        };
        if let Some(code) = refused {
            self.script.steps.borrow_mut().pop_front();
            self.script.note(format!("  refuse {code:?}"));
            return Err(HttpError::with_message(code, b"scripted refusal"));
        }
        Ok(())
    }

    fn poll_read(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRead, HttpError>> {
        self.script.poll(context, &self.cancellation)
    }

    fn poll_start_read(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        self.script.note("  poll_start_read".to_owned());
        if let Err(error) = self.start_read(maximum, allocation_size) {
            return Poll::Ready(Err(error));
        }
        self.poll_read(context)
    }

    fn is_drained(&self) -> bool {
        let drained = self.script.drained.get();
        self.script.note(format!("  is_drained {drained}"));
        drained
    }
}

struct ScriptTransport {
    script: Rc<Script>,
    reader: bool,
}

impl UniplexTransport for ScriptTransport {
    fn try_reader(
        &self,
        _context: &WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexReader>>, HttpError> {
        self.script
            .note(format!("  try_reader timeout={timeout:?}"));
        Ok(self.reader.then(|| {
            Box::new(ScriptReader {
                script: Rc::clone(&self.script),
                cancellation,
            }) as Box<dyn UniplexReader>
        }))
    }

    fn read(
        &self,
        _context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.script.note(format!(
            "  read maximum={maximum} allocation={allocation_size} timeout={timeout:?}"
        ));
        self.script.start();
        Box::pin(ScriptRead {
            script: Rc::clone(&self.script),
            cancellation,
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        _data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        unreachable!("a read slot only reads")
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        unreachable!("a read slot only reads")
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        unreachable!("a read slot only reads")
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        unreachable!("a read slot only reads")
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        unreachable!("a read slot only reads")
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        unreachable!("a read slot only reads")
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Start(usize, usize),
    Poll,
    Cancel,
}

/// One case's inputs, run unchanged through each path.
struct Case {
    name: &'static str,
    steps: &'static [Step],
    ops: &'static [Op],
    panic_on_start: bool,
    panic_on_drop: bool,
    wake_on_drop: bool,
}

/// A waker of its own, so that a read can tell its caller's waker from any other. It counts its
/// wakes, so a transcript shows whether a wake landed before the slot returned.
struct TaskWake(AtomicUsize);

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn outcome(polled: Poll<Result<UniplexRead, HttpError>>) -> String {
    match polled {
        Poll::Pending => "Pending".to_owned(),
        Poll::Ready(Ok(read)) => {
            let eof = read.is_eof();
            let data = read.into_data();
            let bytes = data.coalesce(data.chain_length()).unwrap();
            format!("Ready(Ok {:?} eof={eof})", String::from_utf8_lossy(&bytes))
        }
        Poll::Ready(Err(error)) => format!("Ready(Err {:?} {error})", error.code()),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic".to_owned())
}

/// Runs `case` through the future path (`reader` false) or the reader path, and returns the
/// transcript: each call into the slot, what the transport observed, and the slot's state after.
fn transcript(case: &'static Case, reader: bool) -> Vec<String> {
    run_on_worker(move |context| {
        Box::pin(async move {
            let wakes = Arc::new(TaskWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let script = Rc::new(Script {
                steps: RefCell::new(case.steps.iter().copied().collect()),
                log: RefCell::new(Vec::new()),
                drained: Cell::new(false),
                panic_on_start: Cell::new(false),
                panic_on_drop: Cell::new(case.panic_on_drop),
                wake_on_drop: case.wake_on_drop,
                waker: waker.clone(),
            });
            let transport = ScriptTransport {
                script: Rc::clone(&script),
                reader,
            };
            let source = CancellationSource::new();
            let mut slot = ReadSlot::new(
                &transport,
                &context,
                Duration::from_millis(7),
                source.token(),
            )
            .expect("the slot is built");
            script.note(format!("new uses_reader={}", slot.uses_reader()));
            script.panic_on_start.set(case.panic_on_start);
            for op in case.ops {
                let mut task = Context::from_waker(&waker);
                let polled = catch_unwind(AssertUnwindSafe(|| match op {
                    Op::Start(maximum, allocation_size) => Some(slot.poll_start(
                        &transport,
                        &context,
                        &mut task,
                        *maximum,
                        *allocation_size,
                    )),
                    Op::Poll => Some(slot.poll(&mut task)),
                    Op::Cancel => {
                        source.request_cancellation();
                        None
                    }
                }));
                let mut line = format!("{op:?} -> ");
                match polled {
                    Ok(Some(polled)) => line.push_str(&outcome(polled)),
                    Ok(None) => line.push_str("done"),
                    Err(payload) => {
                        write!(line, "panicked: {}", panic_message(payload.as_ref())).unwrap()
                    }
                }
                write!(
                    line,
                    " reading={} drained={} wakes={}",
                    slot.is_reading(),
                    slot.is_drained(),
                    wakes.0.load(Ordering::Relaxed)
                )
                .unwrap();
                script.note(line);
            }
            drop(slot);
            let unused = script.steps.borrow().len();
            script.note(format!("dropped; unused steps={unused}"));
            script.log.take()
        })
    })
}

const CASES: &[Case] = &[
    Case {
        name: "ready at once",
        steps: &[Step::Data("abc", false)],
        ops: &[Op::Start(64, 16)],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "pending, then data",
        steps: &[Step::Pending, Step::Data("abc", false)],
        ops: &[Op::Start(64, 16), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "drained data, then the next read",
        steps: &[
            Step::Data("ab", true),
            Step::Pending,
            Step::Data("c", false),
        ],
        ops: &[Op::Start(2, 2), Op::Start(9, 4), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "drained data, then an error",
        steps: &[
            Step::Data("ab", true),
            Step::Fail(HttpErrorCode::TRANSPORT_READ_ERROR),
        ],
        ops: &[Op::Start(2, 2), Op::Start(64, 16)],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "drained data, then a refused start",
        steps: &[
            Step::Data("ab", true),
            Step::Refuse(HttpErrorCode::INVALID_STATE_TRANSITION),
        ],
        ops: &[Op::Start(2, 2), Op::Start(64, 16)],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "drained data, then cancellation",
        steps: &[Step::Data("ab", true), Step::Cancellable, Step::Cancellable],
        ops: &[Op::Start(2, 2), Op::Start(64, 16), Op::Cancel, Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "eof after pending",
        steps: &[Step::Pending, Step::Eof],
        ops: &[Op::Start(64, 16), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "error at once, then after pending",
        steps: &[
            Step::Fail(HttpErrorCode::TRANSPORT_READ_ERROR),
            Step::Pending,
            Step::Fail(HttpErrorCode::TRANSPORT_EOF),
        ],
        ops: &[Op::Start(64, 16), Op::Start(64, 16), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "cancellation",
        steps: &[Step::Cancellable, Step::Cancellable],
        ops: &[Op::Start(64, 16), Op::Cancel, Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "refused start",
        steps: &[
            Step::Refuse(HttpErrorCode::INVALID_STATE_TRANSITION),
            Step::Data("after", false),
        ],
        ops: &[Op::Start(64, 16), Op::Start(64, 16)],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "a panicking first poll",
        steps: &[Step::Panic, Step::Data("x", false)],
        ops: &[Op::Start(64, 16), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "a panicking later poll",
        steps: &[Step::Pending, Step::Panic, Step::Data("y", false)],
        ops: &[Op::Start(64, 16), Op::Poll, Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "a panicking start",
        steps: &[Step::Data("z", false)],
        ops: &[Op::Start(64, 16), Op::Poll],
        panic_on_start: true,
        panic_on_drop: false,
        wake_on_drop: false,
    },
    Case {
        name: "a completed future whose drop panics",
        steps: &[Step::Data("d", false), Step::Data("e", false)],
        ops: &[Op::Start(64, 16), Op::Start(64, 16)],
        panic_on_start: false,
        panic_on_drop: true,
        wake_on_drop: false,
    },
    Case {
        name: "completed futures whose drop wakes the task",
        steps: &[
            Step::Data("d", false),
            Step::Pending,
            Step::Data("e", false),
        ],
        ops: &[Op::Start(64, 16), Op::Start(64, 16), Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: true,
    },
    Case {
        name: "a poll with no started read",
        steps: &[],
        ops: &[Op::Poll],
        panic_on_start: false,
        panic_on_drop: false,
        wake_on_drop: false,
    },
];

/// Each case's transcript through the future path, then through the reader path.
const PINNED: &[(&str, &[&str], &[&str])] = &[
    (
        "ready at once",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Data(\"abc\", false) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Ok \"abc\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Data(\"abc\", false) waker=slot's",
            "  is_drained false",
            "Start(64, 16) -> Ready(Ok \"abc\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "pending, then data",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Data(\"abc\", false) waker=slot's",
            "  drop read future",
            "Poll -> Ready(Ok \"abc\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Data(\"abc\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"abc\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "drained data, then the next read",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=2 allocation=2 timeout=7ms",
            "  poll Data(\"ab\", true) waker=slot's",
            "  drop read future",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=false wakes=0",
            "  read maximum=9 allocation=4 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(9, 4) -> Pending reading=true drained=false wakes=0",
            "  poll Data(\"c\", false) waker=slot's",
            "  drop read future",
            "Poll -> Ready(Ok \"c\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=2 allocation=2",
            "  poll Data(\"ab\", true) waker=slot's",
            "  is_drained true",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=true wakes=0",
            "  poll_start_read",
            "  start_read maximum=9 allocation=4",
            "  poll Pending waker=slot's",
            "Start(9, 4) -> Pending reading=true drained=true wakes=0",
            "  poll Data(\"c\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"c\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "drained data, then an error",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=2 allocation=2 timeout=7ms",
            "  poll Data(\"ab\", true) waker=slot's",
            "  drop read future",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Fail(HttpErrorCode(4103)) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4103) scripted failure) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=2 allocation=2",
            "  poll Data(\"ab\", true) waker=slot's",
            "  is_drained true",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=true wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Fail(HttpErrorCode(4103)) waker=slot's",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4103) scripted failure) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "drained data, then a refused start",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=2 allocation=2 timeout=7ms",
            "  poll Data(\"ab\", true) waker=slot's",
            "  drop read future",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Refuse(HttpErrorCode(4096)) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4096) scripted failure) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=2 allocation=2",
            "  poll Data(\"ab\", true) waker=slot's",
            "  is_drained true",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=true wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  refuse HttpErrorCode(4096)",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4096) scripted refusal) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "drained data, then cancellation",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=2 allocation=2 timeout=7ms",
            "  poll Data(\"ab\", true) waker=slot's",
            "  drop read future",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Cancellable waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "Cancel -> done reading=true drained=false wakes=0",
            "  poll Cancellable waker=slot's",
            "  drop read future",
            "Poll -> Ready(Err HttpErrorCode(4101) CORO_CANCELLED) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=2 allocation=2",
            "  poll Data(\"ab\", true) waker=slot's",
            "  is_drained true",
            "Start(2, 2) -> Ready(Ok \"ab\" eof=false) reading=false drained=true wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Cancellable waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=true wakes=0",
            "Cancel -> done reading=true drained=true wakes=0",
            "  poll Cancellable waker=slot's",
            "Poll -> Ready(Err HttpErrorCode(4101) CORO_CANCELLED) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "eof after pending",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Eof waker=slot's",
            "  drop read future",
            "Poll -> Ready(Ok \"\" eof=true) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Eof waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"\" eof=true) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "error at once, then after pending",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Fail(HttpErrorCode(4103)) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4103) scripted failure) reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Fail(HttpErrorCode(4102)) waker=slot's",
            "  drop read future",
            "Poll -> Ready(Err HttpErrorCode(4102) scripted failure) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Fail(HttpErrorCode(4103)) waker=slot's",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4103) scripted failure) reading=false drained=false wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Fail(HttpErrorCode(4102)) waker=slot's",
            "Poll -> Ready(Err HttpErrorCode(4102) scripted failure) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "cancellation",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Cancellable waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "Cancel -> done reading=true drained=false wakes=0",
            "  poll Cancellable waker=slot's",
            "  drop read future",
            "Poll -> Ready(Err HttpErrorCode(4101) CORO_CANCELLED) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Cancellable waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "Cancel -> done reading=true drained=false wakes=0",
            "  poll Cancellable waker=slot's",
            "Poll -> Ready(Err HttpErrorCode(4101) CORO_CANCELLED) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "refused start",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Refuse(HttpErrorCode(4096)) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4096) scripted failure) reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Data(\"after\", false) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Ok \"after\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  refuse HttpErrorCode(4096)",
            "Start(64, 16) -> Ready(Err HttpErrorCode(4096) scripted refusal) reading=false drained=false wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Data(\"after\", false) waker=slot's",
            "  is_drained false",
            "Start(64, 16) -> Ready(Ok \"after\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "a panicking first poll",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Panic waker=slot's",
            "Start(64, 16) -> panicked: scripted poll panic reading=true drained=false wakes=0",
            "  poll Data(\"x\", false) waker=slot's",
            "  drop read future",
            "Poll -> Ready(Ok \"x\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Panic waker=slot's",
            "Start(64, 16) -> panicked: scripted poll panic reading=true drained=false wakes=0",
            "  poll Data(\"x\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"x\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "a panicking later poll",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Panic waker=slot's",
            "Poll -> panicked: scripted poll panic reading=true drained=false wakes=0",
            "  poll Data(\"y\", false) waker=slot's",
            "  drop read future",
            "Poll -> Ready(Ok \"y\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Panic waker=slot's",
            "Poll -> panicked: scripted poll panic reading=true drained=false wakes=0",
            "  poll Data(\"y\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"y\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "a panicking start",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "Start(64, 16) -> panicked: scripted start panic reading=false drained=false wakes=0",
            "Poll -> panicked: a polled read was started reading=false drained=false wakes=0",
            "dropped; unused steps=1",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "Start(64, 16) -> panicked: scripted start panic reading=true drained=false wakes=0",
            "  poll Data(\"z\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"z\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "a completed future whose drop panics",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Data(\"d\", false) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> panicked: scripted drop panic reading=false drained=false wakes=0",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Data(\"e\", false) waker=slot's",
            "  drop read future",
            "Start(64, 16) -> Ready(Ok \"e\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Data(\"d\", false) waker=slot's",
            "  is_drained false",
            "Start(64, 16) -> Ready(Ok \"d\" eof=false) reading=false drained=false wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Data(\"e\", false) waker=slot's",
            "  is_drained false",
            "Start(64, 16) -> Ready(Ok \"e\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "completed futures whose drop wakes the task",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Data(\"d\", false) waker=slot's",
            "  drop read future",
            "  wake",
            "Start(64, 16) -> Ready(Ok \"d\" eof=false) reading=false drained=false wakes=1",
            "  read maximum=64 allocation=16 timeout=7ms",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=1",
            "  poll Data(\"e\", false) waker=slot's",
            "  drop read future",
            "  wake",
            "Poll -> Ready(Ok \"e\" eof=false) reading=false drained=false wakes=2",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Data(\"d\", false) waker=slot's",
            "  is_drained false",
            "Start(64, 16) -> Ready(Ok \"d\" eof=false) reading=false drained=false wakes=0",
            "  poll_start_read",
            "  start_read maximum=64 allocation=16",
            "  poll Pending waker=slot's",
            "Start(64, 16) -> Pending reading=true drained=false wakes=0",
            "  poll Data(\"e\", false) waker=slot's",
            "  is_drained false",
            "Poll -> Ready(Ok \"e\" eof=false) reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
    (
        "a poll with no started read",
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=false",
            "Poll -> panicked: a polled read was started reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
        &[
            "  try_reader timeout=7ms",
            "new uses_reader=true",
            "Poll -> panicked: a polled read was started reading=false drained=false wakes=0",
            "dropped; unused steps=0",
        ],
    ),
];

#[test]
fn read_slot_outcomes_are_pinned_with_and_without_a_reader() {
    let mut captured = String::new();
    let mut mismatches = Vec::new();
    for (index, case) in CASES.iter().enumerate() {
        let future = transcript(case, false);
        let reader = transcript(case, true);
        writeln!(captured, "    (\n        {:?},\n        &[", case.name).unwrap();
        for line in &future {
            writeln!(captured, "            {line:?},").unwrap();
        }
        writeln!(captured, "        ],\n        &[").unwrap();
        for line in &reader {
            writeln!(captured, "            {line:?},").unwrap();
        }
        writeln!(captured, "        ],\n    ),").unwrap();
        match PINNED.get(index) {
            Some((name, pinned_future, pinned_reader))
                if *name == case.name && future == *pinned_future && reader == *pinned_reader => {}
            _ => mismatches.push(case.name),
        }
    }
    assert!(
        mismatches.is_empty() && PINNED.len() == CASES.len(),
        "unpinned or changed outcomes in {mismatches:?}; captured:\n{captured}"
    );
}
