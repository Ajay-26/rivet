mod local;

pub use local::LocalWorker;

use rivet_core::{RivetError, Task, TaskResult, WorkerId};

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
    fn get_id(&self) -> &WorkerId;

    /// Execute a single task and return its result.
    ///
    /// Implementations should update `self.info().status` to `Busy` while
    /// running and back to `Idle` on completion.
    fn execute(&self, task: Task) -> Result<TaskResult, RivetError>;
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::{Task, TaskPayload};

    // -------------------------------------------------------------------------
    // This test will FAIL until you implement LocalWorker::execute.
    // -------------------------------------------------------------------------

    #[test]
    fn worker_execute_returns_success_result() {
        let worker = LocalWorker::new();
        let task = Task::new(TaskPayload::new("noop"));
        let task_id = task.id;
        let result = worker
            .execute(task)
            .expect("execute should not return an error");
        assert!(result.is_success(), "a noop task should succeed");
        assert_eq!(
            result.task_id(),
            task_id,
            "result must reference the original task"
        );
    }
}
