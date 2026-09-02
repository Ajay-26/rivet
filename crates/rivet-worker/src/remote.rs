//! Talking to a worker that lives in another process.
//!
//! Milestone 4 gave the runtime three ways to touch a worker: `send`,
//! `is_alive`, and results arriving on an `mpsc::Receiver`. Nothing else. So a
//! worker on the far end of a socket can be made to look identical, and the
//! runtime's `tick()` never learns the difference.
//!
//! That is what this file is for. `WorkerTransport` is the shared shape, and
//! `RemoteWorkerHandle` is the socket-backed version of it.

use crate::WorkerHandle;
use rivet_core::wire::{
    read_message, write_message, RuntimeToWorker, WorkerToRuntime, PROTOCOL_VERSION,
};
use rivet_core::{RivetError, Task, TaskResult, WorkerId};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

/// Everything the runtime needs from a worker, wherever it is running.
///
/// Same trick as `SchedulerPolicy`: one trait, two implementations, and the
/// caller holds a `Box<dyn ..>` so it does not care which it has.
///
/// `Send` is required because the runtime keeps these inside an
/// `Arc<Mutex<RuntimeInner>>`, and a `Mutex` is only shareable across threads
/// if what it holds can move between them.
pub trait WorkerTransport: std::fmt::Debug + Send {
    /// Hand a task to this worker. Returns once the task is on its way, not
    /// once it has run.
    fn send(&self, task: Task) -> Result<(), RivetError>;

    /// False once this worker can no longer do work. The runtime sweeps for
    /// this at the top of `tick` and marks the worker offline.
    fn is_alive(&self) -> bool;
}

impl WorkerTransport for WorkerHandle {
    /// Forward to the inherent `WorkerHandle::send`.
    /// This is what lets an in-process pool and a remote process sit
    /// in the same map.
    fn send(&self, _task: Task) -> Result<(), RivetError> {
        WorkerHandle::send(self, _task)
    }

    fn is_alive(&self) -> bool {
        WorkerHandle::is_alive(self)
    }
}

/// A worker in another process, reached over TCP.
///
/// Field notes:
///   - `writer` is behind a `Mutex` because several runtime threads may call
///     `send` at once, and two interleaved writes would corrupt the stream.
///     Messages are small, so the lock is held briefly.
///   - `alive` is an `AtomicBool` rather than a plain `bool` because the reader
///     thread writes it while the runtime reads it. It is shared, so it needs
///     `Arc`.
///   - `reader` is `Option` for the same reason `WorkerHandle::inbox` is: `Drop`
///     only gets `&mut self`, so you need `.take()` to get the handle out and
///     join it.
// TODO (Milestone 6, Step 3): remove this allow once the fields are used.
#[allow(dead_code)]
#[derive(Debug)]
pub struct RemoteWorkerHandle {
    id: WorkerId,
    capacity: usize,
    writer: Mutex<TcpStream>,
    alive: Arc<AtomicBool>,
    reader: Option<JoinHandle<()>>,
}

impl RemoteWorkerHandle {
    /// Connect to a worker process and complete the handshake.
    ///
    /// Note who speaks first. The *worker* sends `Hello`, because only it knows
    /// its capacity. The runtime replies `Welcome` with the id it chose —
    /// `worker_id` here — because `WorkerId::new()` is a per-process counter and
    /// two worker processes would otherwise both call themselves `WorkerId(1)`.
    ///
    /// TODO (Milestone 6, Step 3):
    ///   1. `TcpStream::connect(addr)`, then `try_clone` so the reader thread
    ///      gets its own handle. Wrap the read half in a `BufReader`.
    ///   2. Read one `WorkerToRuntime`. It must be `Hello`; anything else is
    ///      `io::ErrorKind::InvalidData`.
    ///   3. Compare its `version` with `rivet_core::wire::PROTOCOL_VERSION` and
    ///      refuse a mismatch. This is the whole reason the field exists — a
    ///      stale worker binary should fail here, loudly, not later in some
    ///      confusing way.
    ///   4. Reply `RuntimeToWorker::Welcome { worker_id }`.
    ///   5. Spawn the reader thread (see `spawn_reader` below).
    ///   6. Build the handle, taking `capacity` from the `Hello`.
    pub fn connect(
        _addr: SocketAddr,
        _worker_id: WorkerId,
        _results: mpsc::Sender<TaskResult>,
    ) -> io::Result<Self> {
        // todo!("Milestone 6, Step 3: connect and handshake")
        let mut stream = TcpStream::connect(_addr)?;
        let mut read_stream = io::BufReader::new(stream.try_clone()?);
        let message =
            read_message::<std::io::BufReader<TcpStream>, WorkerToRuntime>(&mut read_stream)?;
        match message {
            Some(message) => match message {
                WorkerToRuntime::Hello {
                    capacity: _capacity,
                    version: _version,
                } => {
                    if _version != PROTOCOL_VERSION {
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                    let stream_mutex = Mutex::new(stream.try_clone()?);
                    let _ = write_message(
                        &mut stream,
                        &RuntimeToWorker::Welcome {
                            worker_id: _worker_id,
                        },
                    );
                    let is_alive = AtomicBool::new(true);
                    let alive_arc = Arc::new(is_alive);
                    let handle = spawn_reader(stream, _results, alive_arc.clone());
                    return Ok(Self {
                        id: _worker_id,
                        capacity: _capacity,
                        writer: stream_mutex,
                        alive: alive_arc,
                        reader: Some(handle),
                    });
                }
                WorkerToRuntime::Finished(_t) => {
                    return Err(io::ErrorKind::InvalidData.into());
                }
            },
            None => {
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
    }

    /// The id the runtime assigned to this worker.
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// How many tasks this worker can run at once, as it reported in `Hello`.
    ///
    /// The runtime needs this to register `WorkerInfo::new(id).with_capacity(..)`,
    /// or the scheduler will only ever give the worker one task at a time.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Read `WorkerToRuntime`s forever and forward the results to the runtime.
///
/// This one function is what makes a remote worker indistinguishable from a
/// local one. The runtime keeps draining the same `mpsc::Receiver` it always
/// did; this thread is what fills it from a socket instead of from a thread
/// pool.
///
/// TODO (Milestone 6, Step 3):
///   - Loop on `read_message::<_, WorkerToRuntime>`.
///   - `Finished(result)` goes into `results`. If that send fails the runtime
///     is gone, so stop.
///   - A second `Hello` is a protocol error. Log and stop.
///   - `Ok(None)` is a clean hang-up; an `Err` is a broken connection. Both end
///     the loop.
///   - On the way out, set `alive` to `false` with `Ordering::SeqCst`. That flag
///     is the *only* way the runtime finds out this worker died, so it must be
///     set on every exit path, not just the tidy one.
fn spawn_reader(
    _reader: TcpStream,
    _results: mpsc::Sender<TaskResult>,
    _alive: Arc<AtomicBool>,
) -> JoinHandle<()> {
    // todo!("Milestone 6, Step 3: the reader thread");
    let mut reader = io::BufReader::new(_reader);
    std::thread::spawn(move || -> () {
        loop {
            let result = read_message::<_, WorkerToRuntime>(&mut reader);
            match result {
                Ok(Some(result)) => match result {
                    WorkerToRuntime::Hello {
                        capacity: _capacity,
                        version: _version,
                    } => {
                        println!("Protocol Error, wrong message!");
                        break;
                    }
                    WorkerToRuntime::Finished(result) => {
                        let _ = _results
                            .send(result)
                            .map_err(|_err| format!("Error sending tasks {:?}", _err));
                    }
                },
                Ok(None) => {
                    println!("End the loop!");
                    break;
                }
                Err(err) => {
                    println!("Error reading message {:?}", err);
                    break;
                }
            }
        }
        _alive.store(false, std::sync::atomic::Ordering::SeqCst);
    })
}

impl WorkerTransport for RemoteWorkerHandle {
    /// TODO (Milestone 6, Step 3):
    ///   - Lock `writer` and `write_message(&mut *guard, &RuntimeToWorker::Run(task))`.
    ///   - Map the io error into `RivetError::Other`.
    ///   - Consider clearing `alive` here too. A failed write means the socket
    ///     is already gone, and waiting for the reader thread to notice costs
    ///     you a whole tick.
    fn send(&self, _task: Task) -> Result<(), RivetError> {
        // todo!("Milestone 6, Step 3: write RuntimeToWorker::Run to the socket")
        {
            let mut guard = self.writer.lock().unwrap_or_else(|_err| {
                self.alive.store(false, std::sync::atomic::Ordering::SeqCst);
                _err.into_inner()
            });
            let output = write_message(&mut *guard, &RuntimeToWorker::Run(_task));
            if output.is_err() {
                self.alive.store(false, std::sync::atomic::Ordering::SeqCst);
                return Err(RivetError::NoWorkersAvailable);
            }
        }
        return Ok(());
    }

    /// TODO (Milestone 6, Step 3): read the `alive` flag with `Ordering::SeqCst`.
    fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// TODO (Milestone 6, Step 3): implement `Drop` for `RemoteWorkerHandle`.
//
// Closing the socket is what makes the far-end worker's `read_message` return
// `Ok(None)`, which is how the worker process learns to shut down. Use
// `TcpStream::shutdown(Shutdown::Both)`, then `.take()` the reader handle and
// join it. Dropping the stream alone is not enough while the reader thread
// still holds its clone.
impl Drop for RemoteWorkerHandle {
    fn drop(&mut self) {
        {
            let guard = self.writer.lock();
            match guard {
                Ok(guard) => {
                    let res = TcpStream::shutdown(&(*guard), std::net::Shutdown::Both);
                    if res.is_err() {
                        eprintln!("Error shutting down: {:?}!", res.err());
                        return;
                    }
                }
                Err(err) => {
                    eprintln!("Error acquiring lock: {:?}!", err);
                    return;
                }
            }
        }
        match self.reader.take() {
            Some(reader) => {
                let _res = reader.join();
            }
            None => {}
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::wire::{
        read_message, write_message, RuntimeToWorker, WorkerToRuntime, PROTOCOL_VERSION,
    };
    use rivet_core::TaskPayload;
    use std::io::BufReader;
    use std::net::TcpListener;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A stand-in for the worker process, run on a thread inside the test.
    ///
    /// Always bind `127.0.0.1:0` and read the real port back with
    /// `local_addr()`. A hard-coded port collides with other tests and with
    /// whatever else is on the machine.
    struct FakeWorker {
        addr: SocketAddr,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl FakeWorker {
        /// Announce `capacity` and `version`, then echo every task straight back
        /// as a successful result.
        fn start(capacity: usize, version: u32) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let thread = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut writer = stream;

                write_message(&mut writer, &WorkerToRuntime::Hello { capacity, version }).unwrap();
                let welcome = read_message::<_, RuntimeToWorker>(&mut reader).unwrap();
                assert!(
                    matches!(welcome, Some(RuntimeToWorker::Welcome { .. })),
                    "the runtime must reply Welcome, got {welcome:?}"
                );

                while let Ok(Some(RuntimeToWorker::Run(task))) =
                    read_message::<_, RuntimeToWorker>(&mut reader)
                {
                    let result = TaskResult::Success {
                        task_id: task.id,
                        output: task.payload.args,
                    };
                    if write_message(&mut writer, &WorkerToRuntime::Finished(result)).is_err() {
                        break;
                    }
                }
            });

            FakeWorker {
                addr,
                thread: Some(thread),
            }
        }
    }

    impl Drop for FakeWorker {
        fn drop(&mut self) {
            // Detached on purpose, not joined. If the test fails before it
            // connects, this thread is still parked in `accept()`, and joining
            // it would hang the whole suite instead of reporting the failure.
            // A hung suite tells you nothing; a failed test tells you what
            // broke.
            let _ = self.thread.take();
        }
    }

    fn task(name: &str) -> Task {
        Task::new(TaskPayload::new(name))
    }

    #[test]

    fn connect_learns_the_capacity_from_hello() {
        let worker = FakeWorker::start(4, PROTOCOL_VERSION);
        let (tx, _rx) = mpsc::channel();
        let id = WorkerId::new();

        let handle = RemoteWorkerHandle::connect(worker.addr, id, tx).expect("handshake");

        assert_eq!(
            handle.id(),
            id,
            "the runtime chooses the id, not the worker"
        );
        assert_eq!(
            handle.capacity(),
            4,
            "capacity comes from Hello; guessing it means the scheduler \
             under-uses or oversubscribes the worker"
        );
    }

    #[test]

    fn connect_refuses_a_version_mismatch() {
        let worker = FakeWorker::start(1, PROTOCOL_VERSION + 1);
        let (tx, _rx) = mpsc::channel();

        let outcome = RemoteWorkerHandle::connect(worker.addr, WorkerId::new(), tx);
        assert!(
            outcome.is_err(),
            "a worker built from older code must be refused at the handshake, \
             not discovered later through corrupt messages"
        );
    }

    #[test]

    fn a_sent_task_comes_back_as_a_result() {
        let worker = FakeWorker::start(1, PROTOCOL_VERSION);
        let (tx, rx) = mpsc::channel();
        let handle = RemoteWorkerHandle::connect(worker.addr, WorkerId::new(), tx).unwrap();

        let t = task("over-the-wire");
        let id = t.id;
        handle.send(t).expect("send should succeed");

        let result = rx
            .recv_timeout(TIMEOUT)
            .expect("the reader thread must forward results into the runtime's channel");
        assert_eq!(result.task_id(), id);
        assert!(result.is_success());
    }

    #[test]

    fn several_tasks_all_come_back() {
        let worker = FakeWorker::start(2, PROTOCOL_VERSION);
        let (tx, rx) = mpsc::channel();
        let handle = RemoteWorkerHandle::connect(worker.addr, WorkerId::new(), tx).unwrap();

        let mut sent = Vec::new();
        for i in 0..5 {
            let t = task(&format!("job-{i}"));
            sent.push(t.id);
            handle.send(t).unwrap();
        }

        let mut seen = Vec::new();
        for _ in 0..5 {
            seen.push(rx.recv_timeout(TIMEOUT).expect("a result").task_id());
        }
        sent.sort_by_key(|id| id.as_u64());
        seen.sort_by_key(|id| id.as_u64());
        assert_eq!(seen, sent, "every task must come back exactly once");
    }

    #[test]

    fn is_alive_goes_false_when_the_peer_disappears() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // A worker that handshakes, then dies.
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;
            write_message(
                &mut writer,
                &WorkerToRuntime::Hello {
                    capacity: 1,
                    version: PROTOCOL_VERSION,
                },
            )
            .unwrap();
            let _ = read_message::<_, RuntimeToWorker>(&mut reader);
            // drop everything: the socket closes
        });

        let (tx, _rx) = mpsc::channel();
        let handle = RemoteWorkerHandle::connect(addr, WorkerId::new(), tx).unwrap();
        worker.join().unwrap();

        // The reader thread notices the close; give it a moment.
        let deadline = std::time::Instant::now() + TIMEOUT;
        while handle.is_alive() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !handle.is_alive(),
            "a dead peer must show up as is_alive() == false, or the runtime \
             keeps assigning work to a worker that will never answer"
        );
    }

    #[test]

    fn both_kinds_of_worker_fit_in_one_box() {
        // The point of the trait: one map holds local and remote workers alike.
        let (tx, _rx) = mpsc::channel();
        let local: Box<dyn WorkerTransport> = Box::new(crate::spawn(1, tx));
        assert!(local.is_alive());

        let worker = FakeWorker::start(1, PROTOCOL_VERSION);
        let (tx2, _rx2) = mpsc::channel();
        let remote: Box<dyn WorkerTransport> =
            Box::new(RemoteWorkerHandle::connect(worker.addr, WorkerId::new(), tx2).unwrap());
        assert!(remote.is_alive());
    }
}
