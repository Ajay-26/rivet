use crate::Worker;
use rivet_core::{RivetError, Task, TaskResult, WorkerId};

/// A worker that runs tasks in the current process.
///
/// Holds no state beyond its id. Threading lives in `WorkerHandle`, which
/// shares one `LocalWorker` across `capacity` threads, and panic handling lives
/// in the loop inside `spawn`.
#[derive(Debug)]
pub struct LocalWorker {
    id: WorkerId,
}

impl LocalWorker {
    pub fn new() -> Self {
        LocalWorker {
            id: WorkerId::new(),
        }
    }
}

impl Default for LocalWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker for LocalWorker {
    fn get_id(&self) -> &WorkerId {
        &self.id
    }

    fn execute(&self, _task: Task) -> Result<TaskResult, RivetError> {
        // Stand-in for real work. The sleep is load-bearing: the concurrency
        // tests in handle.rs measure elapsed time, so with an instant `execute`
        // they would pass on a sequential implementation too.
        //
        // The "panic" name gives Milestone 5 something to catch.
        std::thread::sleep(std::time::Duration::from_millis(100));
        if _task.payload.name == "panic" {
            panic!("Panic on this specific task!");
        }

        Ok(TaskResult::Success {
            task_id: _task.id,
            output: _task.payload.args,
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::TaskPayload;

    #[test]
    fn each_worker_gets_its_own_id() {
        assert_ne!(
            *LocalWorker::new().get_id(),
            *LocalWorker::new().get_id(),
            "two workers must not share an id, or the scheduler cannot tell them apart"
        );
    }

    #[test]
    fn default_matches_new() {
        // Both must mint an id; this only checks the impl is not a stub.
        let _ = LocalWorker::default();
    }

    /// The output is the round trip the client relies on: bytes in, same bytes
    /// out, under the submitted task's id.
    #[test]
    fn execute_echoes_the_payload_args() {
        let worker = LocalWorker::new();
        let mut payload = TaskPayload::new("echo");
        payload.args = vec![1, 2, 3];
        let task = Task::new(payload);
        let id = task.id;

        let result = worker.execute(task).expect("execute should not error");

        match result {
            TaskResult::Success { task_id, output } => {
                assert_eq!(task_id, id, "the result must name the task it came from");
                assert_eq!(output, vec![1, 2, 3]);
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    /// Milestone 2 claims `execute` does measurable work. If the sleep is
    /// removed, the concurrency tests in `handle.rs` stop discriminating —
    /// they would pass on a sequential implementation too.
    #[test]
    fn execute_takes_measurable_time() {
        let worker = LocalWorker::new();
        let start = std::time::Instant::now();
        let _ = worker.execute(Task::new(TaskPayload::new("noop")));
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(50),
            "execute returned instantly, so the timing tests in handle.rs no \
             longer prove anything about concurrency"
        );
    }

    #[test]
    fn the_panic_payload_name_panics() {
        let worker = LocalWorker::new();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            worker.execute(Task::new(TaskPayload::new("panic")))
        }));
        assert!(
            outcome.is_err(),
            "Milestone 5 needs a task that panics on demand; without it the \
             fault-tolerance tests have nothing to catch"
        );
    }

    /// `spawn` shares one `LocalWorker` across `capacity` threads via `Arc`,
    /// which only compiles if the worker is `Send + Sync`.
    #[test]
    fn local_worker_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LocalWorker>();
    }
}
