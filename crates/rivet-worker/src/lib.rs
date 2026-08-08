mod local;

pub use local::LocalWorker;

use rivet_core::{RivetError, Task, TaskResult, WorkerInfo};

/// The contract every worker implementation must satisfy.
///
/// A worker's lifecycle:
///
///   1. Come online (construction / registration with the scheduler).
///   2. Receive a task assignment.
///   3. Execute the task.
///   4. Report the result back to the scheduler.
///
/// # Design questions to think about
///
/// TODO: Should `execute` be synchronous (blocking the calling thread),
/// asynchronous (returning a `Future`), or offloaded to a thread pool?
/// This decision determines how many tasks a single worker can overlap.
///
/// TODO: How should a worker signal that it is ready for another task?
/// Push (worker calls back to the scheduler) or pull (scheduler polls)?
pub trait Worker {
    /// Return read-only metadata about this worker.
    fn info(&self) -> &WorkerInfo;

    /// Execute a single task and return its result.
    ///
    /// Implementations should update `self.info().status` to `Busy` while
    /// running and back to `Idle` on completion.
    fn execute(&mut self, task: Task) -> Result<TaskResult, RivetError>;
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::{TaskPayload, Task, WorkerStatus};

    #[test]
    fn worker_starts_idle() {
        let worker = LocalWorker::new();
        assert_eq!(
            worker.info().status,
            WorkerStatus::Idle,
            "a freshly created worker should be idle"
        );
    }

    // -------------------------------------------------------------------------
    // This test will FAIL until you implement LocalWorker::execute.
    // -------------------------------------------------------------------------

    #[test]
    fn worker_execute_returns_success_result() {
        let mut worker = LocalWorker::new();
        let task = Task::new(TaskPayload::new("noop"));
        let task_id = task.id;
        let result = worker.execute(task).expect("execute should not return an error");
        assert!(result.is_success(), "a noop task should succeed");
        assert_eq!(result.task_id(), task_id, "result must reference the original task");
    }

    #[test]
    fn worker_returns_to_idle_after_execution() {
        // TODO (Milestone 2): Uncomment and implement this test once execute works.
        //
        // let mut worker = LocalWorker::new();
        // let task = Task::new(TaskPayload::new("noop"));
        // worker.execute(task).unwrap();
        // assert_eq!(worker.info().status, WorkerStatus::Idle);
    }
}
