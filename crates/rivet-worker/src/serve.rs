//! Serving the worker over a socket.
//!
//! The logic lives here, in the library, rather than in `src/bin/rivet-worker.rs`.
//! A binary's internals cannot be reached from a test — nothing can `use` a
//! `main` — so a binary that holds real logic is a binary you cannot test.
//! Keep `main` to argument parsing and a call into this module.

use crate::spawn;
use rivet_core::wire::{
    read_message, write_message, RuntimeToWorker, WorkerToRuntime, PROTOCOL_VERSION,
};
use rivet_core::{TaskResult, WorkerId};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

/// What the worker binary was asked to do.
#[derive(Debug, PartialEq, Eq)]
pub struct Config {
    pub addr: SocketAddr,
    pub capacity: usize,
}

/// Parse `rivet-worker <addr> [capacity]`.
///
/// A free function over a slice, not something that reads `std::env` itself,
/// so a test can call it with any argument list.
///
/// Milestone 6:
///   - `args` excludes the program name; the caller strips it.
///   - `addr` parses with `str::parse::<SocketAddr>()`. Report the bad input in
///     the error string — "invalid address" alone helps nobody.
///   - `capacity` is optional and defaults to 1. Reject 0: a worker with no
///     threads accepts tasks and never runs them.
pub fn parse_args(_args: &[String]) -> Result<Config, String> {
    if _args.is_empty() {
        return Err(String::from("Empty argument list"));
    }
    let addr = (_args[0].as_str()).parse::<SocketAddr>();
    if addr.is_err() {
        return Err(String::from(
            "Couldn't parse first argument into socket address",
        ));
    }

    let capacity = match _args.len() > 1 {
        true => _args[1]
            .parse::<usize>()
            .map_err(|_| "Incorrect format for capacity argument")?,
        false => 1,
    };
    if capacity == 0 {
        return Err(format!("Wrong value of capacity provided {:?}", capacity));
    }
    return Ok(Config {
        addr: addr.unwrap(),
        capacity: capacity,
    });
}

/// Accept connections on `listener` and serve each one in turn.
///
/// Milestone 6:
///   - One connection at a time is fine; the runtime opens exactly one per
///     worker. Loop on `listener.incoming()` so a dropped connection does not
///     end the process.
///   - A failed connection is not a failed worker. Log it and keep accepting.
pub fn serve(_listener: &TcpListener, _capacity: usize) -> io::Result<()> {
    for stream in _listener.incoming() {
        match stream {
            Ok(stream) => {
                let res = serve_connection(stream, _capacity);
                if res.is_err() {
                    eprintln!("Found error, {:?}", res.err());
                }
            }
            Err(e) => {
                eprintln!("Found error, {:?}", e);
            }
        }
    }
    return Ok(());
}

/// Agree on a protocol version and learn the id the runtime assigned.
///
/// Split out from `serve_connection`, and generic over the two halves rather
/// than taking a `TcpStream`, so its tests need no socket at all — a pair of
/// in-memory buffers is enough.
///
/// Milestone 6:
///   - Send `WorkerToRuntime::Hello { capacity, version: PROTOCOL_VERSION }` first.
///     worker speaks first because the runtime cannot know the capacity.
///   - Expect `RuntimeToWorker::Welcome { worker_id }` back. Anything else, or a clean
///     EOF, is a protocol error — `io::ErrorKind::InvalidData`.
pub fn handshake<R: io::BufRead, W: io::Write>(
    _reader: &mut R,
    _writer: &mut W,
    _capacity: usize,
) -> io::Result<WorkerId> {
    let send_req = WorkerToRuntime::Hello {
        capacity: _capacity,
        version: PROTOCOL_VERSION,
    };
    write_message(_writer, &send_req)?;

    let recv_response = read_message::<R, RuntimeToWorker>(_reader)?;
    if recv_response.is_none() {
        return Err(io::ErrorKind::InvalidData.into());
    }
    match recv_response.unwrap() {
        RuntimeToWorker::Welcome { worker_id } => {
            return Ok(worker_id);
        }
        RuntimeToWorker::Run(_) => {
            return Err(io::ErrorKind::InvalidData.into());
        }
    }
}

/// Run the protocol on one accepted connection until the peer hangs up.
///
/// This is the pump. Note what it does *not* do: it never executes a task
/// itself. `spawn` already gives you a thread pool with panic handling and a
/// results channel, so this function only moves messages between that pool and
/// the socket.
///
/// Milestone 6:
///   1. `stream.try_clone()` — you need two owned handles to the same socket,
///      because one thread reads while another writes. Wrap the read half in a
///      `BufReader`; `read_message` needs `BufRead`, and an unbuffered
///      `TcpStream` does not implement it.
///   2. `handshake` to get the assigned `WorkerId`.
///   3. `let (results_tx, results_rx) = mpsc::channel();`
///      `let handle = spawn(capacity, results_tx);`
///   4. Spawn a writer thread that drains `results_rx` and writes each result
///      out as `WorkerToRuntime::Finished`. It has to be its own thread: reading and
///      writing both block, and a single thread doing both deadlocks the moment
///      a task takes longer than the next request takes to arrive.
///   5. Read `RuntimeToWorker`s in a loop. `Run(task)` becomes `handle.send(task)`.
///      A second `Hello` is a protocol error.
///   6. `read_message` returning `Ok(None)` is the runtime closing down. Leave
///      the loop; do not treat it as a failure.
///
/// Shut down in this order, and work out for yourself why each step unblocks
/// the next:
///   - drop `handle` (its `Drop` closes the inbox and joins the task threads)
///   - drop the last `results_tx`
///   - join the writer thread
pub fn serve_connection(_stream: TcpStream, _capacity: usize) -> io::Result<()> {
    let mut reader = io::BufReader::new(_stream.try_clone()?);
    let mut writer = io::BufWriter::new(_stream.try_clone()?);
    handshake(&mut reader, &mut writer, _capacity)?;
    let (results_sender, results_receiver) = std::sync::mpsc::channel::<TaskResult>();
    let handle = spawn(_capacity, results_sender);

    let thread_handle = std::thread::spawn(move || -> () {
        loop {
            let result = results_receiver.recv();
            match result {
                Ok(result) => {
                    let result = WorkerToRuntime::Finished(result);
                    let out = write_message(&mut writer, &result);
                    if out.is_err() {
                        eprintln!("rivet-worker: could not send result");
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    });

    loop {
        let request = read_message::<io::BufReader<TcpStream>, RuntimeToWorker>(&mut reader);
        if request.is_err() {
            break;
        }
        match request.unwrap() {
            Some(request) => match request {
                RuntimeToWorker::Welcome { worker_id: _ } => {
                    eprintln!("Wrong format for request!");
                }
                RuntimeToWorker::Run(t) => {
                    let send = handle.send(t);
                    if send.is_err() {
                        eprintln!("Got error {:?}", send.err());
                    }
                }
            },
            None => {
                break;
            }
        }
    }
    drop(handle);
    let ret = thread_handle.join();
    if ret.is_err() {
        eprintln!("Error joining threads");
    }
    return Ok(());
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::wire::{read_message, write_message, RuntimeToWorker, WorkerToRuntime};
    use rivet_core::{Task, TaskPayload};

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ── parse_args ───────────────────────────────────────────────────────────

    #[test]
    fn an_address_alone_defaults_to_one_thread() {
        // The everyday invocation: `rivet-worker 127.0.0.1:7001`.
        let config = parse_args(&args(&["127.0.0.1:7001"])).expect("one argument is valid");
        assert_eq!(config.addr, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(config.capacity, 1, "capacity is optional and defaults to 1");
    }

    #[test]
    fn a_capacity_argument_is_used() {
        let config = parse_args(&args(&["127.0.0.1:7001", "4"])).unwrap();
        assert_eq!(config.capacity, 4);
    }

    #[test]
    fn no_arguments_is_an_error_not_a_crash() {
        // Indexing before checking the length would panic here instead.
        assert!(parse_args(&[]).is_err());
    }

    #[test]
    fn a_bad_address_is_rejected() {
        for bad in ["localhost:7001", "127.0.0.1", "7001", "not-an-address"] {
            assert!(
                parse_args(&args(&[bad])).is_err(),
                "{bad:?} is not a SocketAddr and should be refused at startup, \
                 not halfway through a run"
            );
        }
    }

    #[test]
    fn a_non_numeric_capacity_is_rejected() {
        assert!(parse_args(&args(&["127.0.0.1:7001", "two"])).is_err());
    }

    #[test]
    fn a_capacity_of_zero_is_rejected() {
        assert!(
            parse_args(&args(&["127.0.0.1:7001", "0"])).is_err(),
            "a worker with no threads accepts tasks and never runs them"
        );
    }

    // ── handshake ────────────────────────────────────────────────────────────

    /// Build the reader side from whatever the runtime is pretending to say.
    fn runtime_says(response: Option<&RuntimeToWorker>) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(r) = response {
            write_message(&mut buf, r).unwrap();
        }
        buf
    }

    #[test]
    fn handshake_sends_hello_first_with_our_capacity_and_version() {
        let assigned = WorkerId::new();
        let incoming = runtime_says(Some(&RuntimeToWorker::Welcome {
            worker_id: assigned,
        }));
        let mut sent = Vec::new();

        handshake(&mut incoming.as_slice(), &mut sent, 3).expect("a Welcome should be accepted");

        // The worker speaks first, because the runtime cannot know the capacity.
        let first = read_message::<_, WorkerToRuntime>(&mut sent.as_slice())
            .unwrap()
            .expect("the worker must send something before it waits");
        match first {
            WorkerToRuntime::Hello { capacity, version } => {
                assert_eq!(capacity, 3, "the runtime sizes the pool from this number");
                assert_eq!(version, PROTOCOL_VERSION);
            }
            other => panic!("the first message must be Hello, got {other:?}"),
        }
    }

    #[test]
    fn handshake_returns_the_id_the_runtime_assigned() {
        let assigned = WorkerId::new();
        let incoming = runtime_says(Some(&RuntimeToWorker::Welcome {
            worker_id: assigned,
        }));
        let mut sent = Vec::new();

        let got = handshake(&mut incoming.as_slice(), &mut sent, 1).unwrap();
        assert_eq!(
            got, assigned,
            "ids are minted by the runtime; two worker processes would otherwise \
             both call themselves WorkerId(1)"
        );
    }

    #[test]
    fn handshake_rejects_a_reply_that_is_not_welcome() {
        // A Run before the handshake finished: the runtime is out of step.
        let wrong = RuntimeToWorker::Run(Task::new(TaskPayload::new("early")));
        let incoming = runtime_says(Some(&wrong));
        let mut sent = Vec::new();

        let outcome = handshake(&mut incoming.as_slice(), &mut sent, 1);
        assert!(
            outcome.is_err(),
            "a result before the handshake means the peer is confused; serving it \
             anyway hides the real problem"
        );
    }

    #[test]
    fn handshake_fails_when_the_peer_hangs_up() {
        let incoming = runtime_says(None); // empty: connection closed, no reply
        let mut sent = Vec::new();

        let outcome = handshake(&mut incoming.as_slice(), &mut sent, 1);
        assert!(
            outcome.is_err(),
            "silence is not consent — without a Welcome the worker has no id and \
             no confirmation that its version was accepted"
        );
    }

    #[test]
    fn handshake_reports_a_protocol_error_as_invalid_data() {
        let incoming = runtime_says(None);
        let mut sent = Vec::new();

        let error = handshake(&mut incoming.as_slice(), &mut sent, 1).unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidData,
            "the caller distinguishes a broken peer from a broken socket by kind"
        );
    }
}
