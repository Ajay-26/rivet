//! Milestone 6, end to end: real worker processes over real sockets.
//!
//! These are the only tests that prove the whole path works. Everything else
//! stubs one side out.
//!
//! Two habits they rely on:
//!
//!   - Bind port 0 and read the real port back. A hard-coded port collides with
//!     other tests, with parallel `cargo test` runs, and with whatever else is
//!     on the machine. `rivet-worker` prints `listening on <addr>` for exactly
//!     this reason.
//!   - Kill child processes even when the test fails. A panicking test that
//!     leaks workers leaves ports held and the next run fails for the wrong
//!     reason. `Drop` runs during a panic, so put the kill there.
//!
//! Finding the binary: cargo only defines `CARGO_BIN_EXE_<name>` for tests in
//! the crate that *declares* that binary. `rivet-worker` is declared in the
//! worker crate, not here, so we locate it from the test binary's own path
//! instead. Run `cargo test --workspace`, or `cargo build` first — a bare
//! `cargo test -p rivet-client` will not build the worker binary.

use rivet_client::{Client, LocalRuntime};
use rivet_core::TaskPayload;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A real `rivet-worker` process.
struct WorkerProcess {
    child: Child,
    addr: SocketAddr,
}

impl WorkerProcess {
    /// Where cargo put the worker binary.
    ///
    /// This test binary lives in `target/<profile>/deps/`, so the sibling
    /// binaries are one directory up. Deriving it this way keeps debug and
    /// release working without hard-coding either.
    fn binary() -> std::path::PathBuf {
        let mut path = std::env::current_exe().expect("the test binary has a path");
        path.pop(); // out of deps/
        if path.ends_with("deps") {
            path.pop();
        }
        path.push("rivet-worker");
        assert!(
            path.exists(),
            "{} is missing. Run `cargo test --workspace`, or `cargo build` \
             first — `cargo test -p rivet-client` alone does not build the \
             worker binary.",
            path.display()
        );
        path
    }

    /// Start a worker on a port the OS picks, and wait until it is listening.
    fn start(capacity: usize) -> WorkerProcess {
        let mut child = Command::new(Self::binary())
            .arg("127.0.0.1:0")
            .arg(capacity.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .expect("the worker binary should start");

        let stdout = child.stdout.take().expect("stdout was piped");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("the worker should announce its address before serving");

        let addr = line
            .trim()
            .strip_prefix("listening on ")
            .unwrap_or_else(|| panic!("unexpected first line from the worker: {line:?}"))
            .parse()
            .expect("the announced address should parse");

        WorkerProcess { child, addr }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Tick until every id has a result, or give up. Bounded so a stalled task
/// reports a failure instead of hanging the suite.
fn tick_until_all(
    runtime: &mut LocalRuntime,
    client: &impl Client,
    ids: &[rivet_core::TaskId],
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        runtime.tick();
        if ids
            .iter()
            .all(|id| client.get_result(*id).unwrap().is_some())
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn a_worker_process_starts_and_announces_its_port() {
    let worker = WorkerProcess::start(1);
    assert_eq!(worker.addr.ip().to_string(), "127.0.0.1");
    assert_ne!(
        worker.addr.port(),
        0,
        "port 0 must be resolved to a real port"
    );
}

#[test]
fn a_task_runs_in_another_process() {
    let worker = WorkerProcess::start(1);
    let mut runtime = LocalRuntime::with_remote_workers(&[worker.addr]).expect("connect");
    let mut client = runtime.client();

    let id = client.submit(TaskPayload::new("over-the-wire")).unwrap();
    assert!(
        tick_until_all(&mut runtime, &client, &[id]),
        "the task never came back from the worker process"
    );
    assert!(client.get_result(id).unwrap().unwrap().is_success());
}

#[test]
fn two_worker_processes_get_distinct_ids() {
    // Both processes mint WorkerId(1) internally, because the counter is
    // per-process. If the runtime trusted the worker's own id, the second
    // registration would collide with the first and one worker would vanish.
    let a = WorkerProcess::start(1);
    let b = WorkerProcess::start(1);

    let mut runtime = LocalRuntime::with_remote_workers(&[a.addr, b.addr]).expect("connect both");
    let mut client = runtime.client();

    let ids: Vec<_> = (0..4)
        .map(|i| {
            client
                .submit(TaskPayload::new(&format!("job-{i}")))
                .unwrap()
        })
        .collect();

    assert!(
        tick_until_all(&mut runtime, &client, &ids),
        "with two workers registered under distinct ids, all four should finish"
    );
}

#[test]
fn a_workers_capacity_comes_from_its_handshake() {
    // Capacity 3 means the scheduler may give this worker three tasks at once.
    // If the runtime guesses 1 instead, these run one after another and the
    // elapsed time gives it away.
    let worker = WorkerProcess::start(3);
    let mut runtime = LocalRuntime::with_remote_workers(&[worker.addr]).expect("connect");
    let mut client = runtime.client();

    let ids: Vec<_> = (0..3)
        .map(|i| {
            client
                .submit(TaskPayload::new(&format!("job-{i}")))
                .unwrap()
        })
        .collect();

    let start = Instant::now();
    assert!(
        tick_until_all(&mut runtime, &client, &ids),
        "all three should finish"
    );
    let elapsed = start.elapsed();

    // Each task sleeps ~100ms. Three in parallel is ~100ms, sequential is ~300ms.
    assert!(
        elapsed < Duration::from_millis(280),
        "three tasks took {elapsed:?}; the worker announced capacity 3 but the \
         scheduler is only giving it one at a time"
    );
}

#[test]
fn a_killed_worker_process_does_not_strand_its_tasks() {
    // This is the failure Milestone 5's is_alive() sweep was written for and
    // could never be tested in-process: a caught panic never kills a thread,
    // but `kill -9` really does kill a process.
    let mut victim = WorkerProcess::start(1);
    let survivor = WorkerProcess::start(1);

    let mut runtime =
        LocalRuntime::with_remote_workers(&[victim.addr, survivor.addr]).expect("connect both");
    let mut client = runtime.client();

    let ids: Vec<_> = (0..4)
        .map(|i| {
            client
                .submit(TaskPayload::new(&format!("job-{i}")))
                .unwrap()
        })
        .collect();

    runtime.tick(); // hand work out
    victim.kill(); // one machine dies mid-flight

    assert!(
        tick_until_all(&mut runtime, &client, &ids),
        "tasks in flight on the dead worker were never requeued. The runtime \
         must notice is_alive() == false, mark that worker offline, and put its \
         tasks back on the queue for the survivor."
    );
}

#[test]
fn connecting_to_a_dead_address_is_an_error() {
    // Bind a port, then drop the listener so nothing is there any more.
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let outcome = LocalRuntime::with_remote_workers(&[addr]);
    assert!(
        outcome.is_err(),
        "a runtime that silently starts with zero workers accepts tasks and \
         never runs them"
    );
}
