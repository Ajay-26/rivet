use crate::{Task, TaskResult, WorkerId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{BufRead, Write};

pub const PROTOCOL_VERSION: u32 = 1;

/// Everything a worker ever sends.
///
/// Named for the direction, not for "request" and "reply". Both sides send
/// both kinds of thing, so direction is the only grouping that stays true:
/// if a message is in this enum, the worker wrote it.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerToRuntime {
    /// First message on a new connection. The worker speaks first because only
    /// it knows its own capacity.
    Hello { capacity: usize, version: u32 },
    /// A task finished, one way or the other.
    Finished(TaskResult),
}

/// Everything the runtime ever sends.
#[derive(Debug, Serialize, Deserialize)]
pub enum RuntimeToWorker {
    /// Reply to `Hello`. Carries the id the runtime assigned, and doubles as
    /// confirmation that the version was accepted.
    Welcome { worker_id: WorkerId },
    /// Run this task.
    Run(Task),
}

pub fn write_message<W: Write, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *w, msg)?;
    w.write_all(b"\n")?;
    w.flush()?;
    return Ok(());
}

pub fn read_message<R: BufRead, T: DeserializeOwned>(r: &mut R) -> io::Result<Option<T>> {
    let mut line_buffer = String::new();

    let bytes_read = r.read_line(&mut line_buffer);
    match bytes_read {
        Ok(bytes_read) => {
            if bytes_read == 0 {
                return Ok(None);
            } else {
                let result = serde_json::from_str::<T>(&line_buffer)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok(Some(result));
            }
        }
        Err(e) => {
            return Err(e);
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskId, TaskPayload};
    use std::io::{BufReader, Read};

    fn task(name: &str) -> Task {
        Task::new(TaskPayload::new(name).with_args(vec![1, 2, 3]))
    }

    /// A reader that hands back at most three bytes per call, the way a real
    /// socket does. `Vec<u8>` and `&[u8]` always return everything at once, so
    /// a test built only on those cannot detect a framing bug.
    struct Dribble {
        bytes: Vec<u8>,
        pos: usize,
    }

    impl Read for Dribble {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = (self.bytes.len() - self.pos).min(buf.len()).min(3);
            buf[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn wire_round_trips_every_message() {
        let sent = task("round-trip");
        let task_id = sent.id;
        let worker_id = WorkerId::new();

        // Everything the worker sends.
        let mut buf = Vec::new();
        write_message(
            &mut buf,
            &WorkerToRuntime::Hello {
                capacity: 4,
                version: 1,
            },
        )
        .unwrap();
        write_message(
            &mut buf,
            &WorkerToRuntime::Finished(TaskResult::Success {
                task_id,
                output: vec![7],
            }),
        )
        .unwrap();

        let mut reader = buf.as_slice();
        match read_message::<_, WorkerToRuntime>(&mut reader)
            .unwrap()
            .unwrap()
        {
            WorkerToRuntime::Hello { capacity, version } => {
                assert_eq!((capacity, version), (4, 1));
            }
            other => panic!("expected Hello, got {other:?}"),
        }
        match read_message::<_, WorkerToRuntime>(&mut reader)
            .unwrap()
            .unwrap()
        {
            WorkerToRuntime::Finished(result) => {
                assert_eq!(result.task_id(), task_id);
                assert!(result.is_success());
            }
            other => panic!("expected Finished, got {other:?}"),
        }

        // Everything the runtime sends.
        let mut buf = Vec::new();
        write_message(&mut buf, &RuntimeToWorker::Welcome { worker_id }).unwrap();
        write_message(&mut buf, &RuntimeToWorker::Run(sent)).unwrap();

        let mut reader = buf.as_slice();
        match read_message::<_, RuntimeToWorker>(&mut reader)
            .unwrap()
            .unwrap()
        {
            RuntimeToWorker::Welcome { worker_id: got } => assert_eq!(got, worker_id),
            other => panic!("expected Welcome, got {other:?}"),
        }
        match read_message::<_, RuntimeToWorker>(&mut reader)
            .unwrap()
            .unwrap()
        {
            RuntimeToWorker::Run(t) => assert_eq!(t.id, task_id),
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn a_task_survives_the_round_trip_intact() {
        let sent = task("payload-check");
        let (id, name, args) = (
            sent.id,
            sent.payload.name.clone(),
            sent.payload.args.clone(),
        );

        let mut buf = Vec::new();
        write_message(&mut buf, &RuntimeToWorker::Run(sent)).unwrap();
        let got = read_message::<_, RuntimeToWorker>(&mut buf.as_slice())
            .unwrap()
            .unwrap();

        match got {
            RuntimeToWorker::Run(t) => {
                assert_eq!(
                    t.id, id,
                    "the id must survive, or results cannot be matched up"
                );
                assert_eq!(t.payload.name, name);
                assert_eq!(
                    t.payload.args, args,
                    "Vec<u8> travels as a JSON number array"
                );
                assert_eq!(t.status, crate::TaskStatus::Pending);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    /// The framing test. A socket splits writes wherever it likes, so the
    /// reader has to keep going until it finds the delimiter.
    #[test]
    fn read_message_reassembles_a_split_write() {
        let mut buf = Vec::new();
        write_message(&mut buf, &RuntimeToWorker::Run(task("chunked"))).unwrap();
        assert!(buf.len() > 3, "the message must be longer than one dribble");

        let mut reader = BufReader::new(Dribble { bytes: buf, pos: 0 });
        let got = read_message::<_, RuntimeToWorker>(&mut reader)
            .expect("a split write is not an error")
            .expect("three bytes at a time is still one whole message");
        assert!(matches!(got, RuntimeToWorker::Run(_)));
    }

    #[test]
    fn two_messages_in_one_buffer_read_back_as_two() {
        let mut buf = Vec::new();
        write_message(
            &mut buf,
            &WorkerToRuntime::Hello {
                capacity: 1,
                version: 1,
            },
        )
        .unwrap();
        write_message(
            &mut buf,
            &WorkerToRuntime::Hello {
                capacity: 2,
                version: 1,
            },
        )
        .unwrap();

        let mut reader = buf.as_slice();
        let mut seen = Vec::new();
        while let Some(WorkerToRuntime::Hello { capacity, .. }) =
            read_message::<_, WorkerToRuntime>(&mut reader).unwrap()
        {
            seen.push(capacity);
        }
        assert_eq!(
            seen,
            vec![1, 2],
            "a reader that swallows the rest of the buffer loses the second message"
        );
    }

    #[test]
    fn read_message_returns_none_at_eof() {
        let mut empty = &b""[..];
        let got = read_message::<_, WorkerToRuntime>(&mut empty).expect("EOF is not an error");
        assert!(got.is_none(), "a peer that hung up cleanly reads as None");
    }

    #[test]
    fn write_message_appends_exactly_one_trailing_newline() {
        let mut buf = Vec::new();
        write_message(
            &mut buf,
            &WorkerToRuntime::Hello {
                capacity: 1,
                version: 1,
            },
        )
        .unwrap();

        assert_eq!(
            buf.iter().filter(|b| **b == b'\n').count(),
            1,
            "one newline per message, or the frames do not line up"
        );
        assert_eq!(*buf.last().unwrap(), b'\n', "the delimiter must come last");
    }

    #[test]
    fn malformed_json_is_an_error() {
        let mut bad = &b"{ not json }\n"[..];
        let got = read_message::<_, WorkerToRuntime>(&mut bad);
        assert!(
            got.is_err(),
            "a peer speaking a different protocol must surface as an error"
        );
    }

    /// A message whose payload contains a newline. JSON escapes it inside the
    /// string, so it must not be mistaken for a frame boundary. This is the
    /// assumption newline-delimited framing rests on.
    #[test]
    fn a_newline_inside_the_payload_does_not_split_the_frame() {
        let payload = TaskPayload::new("line\none\nline\ntwo");
        let sent = Task::new(payload);

        let mut buf = Vec::new();
        write_message(&mut buf, &RuntimeToWorker::Run(sent)).unwrap();
        assert_eq!(
            buf.iter().filter(|b| **b == b'\n').count(),
            1,
            "the newlines in the name must be escaped, not written raw"
        );

        let got = read_message::<_, RuntimeToWorker>(&mut buf.as_slice())
            .unwrap()
            .unwrap();
        match got {
            RuntimeToWorker::Run(t) => assert_eq!(t.payload.name, "line\none\nline\ntwo"),
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_task_id_is_still_a_valid_message() {
        // TaskId is a plain counter, so a worker will happily deserialize an id
        // it has never seen. Nothing on the wire prevents that; the scheduler is
        // what rejects it. Recorded here so the behaviour is a decision.
        let orphan = TaskId::new();
        let mut buf = Vec::new();
        write_message(
            &mut buf,
            &WorkerToRuntime::Finished(TaskResult::Failure {
                task_id: orphan,
                error: String::from("boom"),
            }),
        )
        .unwrap();
        let got = read_message::<_, WorkerToRuntime>(&mut buf.as_slice())
            .unwrap()
            .unwrap();
        match got {
            WorkerToRuntime::Finished(r) => assert_eq!(r.task_id(), orphan),
            other => panic!("expected Finished, got {other:?}"),
        }
    }
}
