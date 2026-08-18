mod local;
mod policy;

pub use local::LocalScheduler;

use rivet_core::{RivetError, Task, TaskId, TaskResult, WorkerId, WorkerInfo};

/// Maps a single task to the worker that should execute it.
#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub task: Task,
}

/// The core scheduling contract.
///
/// The scheduler is the central coordinator. It knows:
///   - which tasks are waiting to run
///   - which workers exist and whether they are available
///   - which worker should receive each task
///
/// # `&mut self`
/// Methods take `&mut self` because scheduling reads and writes internal state.
///
/// TODO: If the scheduler needs to be shared across threads (e.g. a background
/// scheduling loop + a submission thread), it will need interior mutability.
/// Think about whether `Arc<Mutex<dyn Scheduler>>` or a message-passing design
/// is the right approach for your use case.
pub trait Scheduler {
    /// Accept a new task. Returns the ID assigned to it.
    fn submit(&mut self, task: Task) -> TaskId;

    /// Compute a list of task-to-worker assignments for all pending tasks
    /// that can currently be dispatched.
    ///
    /// This is the heart of the scheduler. A simple first pass:
    /// give each idle worker one pending task, in submission order.
    ///
    /// TODO: Consider richer policies: priority queues, locality awareness,
    /// work stealing, fair-share scheduling.
    fn schedule(&mut self) -> Vec<TaskAssignment>;

    /// Called when a worker comes online and announces itself.
    fn worker_registered(&mut self, worker: WorkerInfo) -> Result<(), RivetError>;

    /// Called when a worker reports it has finished executing a task.
    /// The scheduler uses this to update task state and mark the worker idle.
    fn worker_finished(&mut self, result: TaskResult) -> Result<(), RivetError>;
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::{Task, TaskPayload, WorkerId};

    // -------------------------------------------------------------------------
    // These tests will FAIL until you implement LocalScheduler.
    // Your goal for Milestone 1 is to make every test in this file pass.
    // -------------------------------------------------------------------------

    #[test]
    fn scheduler_submit_preserves_task_id() {
        let mut scheduler = LocalScheduler::new();
        let task = Task::new(TaskPayload::new("greet"));
        let expected_id = task.id;
        let returned_id = scheduler.submit(task);
        assert_eq!(
            expected_id, returned_id,
            "submit should return the ID that was already on the task"
        );
    }

    #[test]
    fn scheduler_produces_no_assignments_without_workers() {
        let mut scheduler = LocalScheduler::new();
        scheduler.submit(Task::new(TaskPayload::new("lonely")));
        let assignments = scheduler.schedule();
        assert!(
            assignments.is_empty(),
            "no workers registered → no assignments possible"
        );
    }

    #[test]
    fn scheduler_registers_worker_without_error() {
        let mut scheduler = LocalScheduler::new();
        let worker = WorkerInfo::new(WorkerId::new());
        assert!(scheduler.worker_registered(worker).is_ok());
    }

    #[test]
    fn scheduler_assigns_task_to_available_worker() {
        let mut scheduler = LocalScheduler::new();

        let worker_id = WorkerId::new();
        scheduler
            .worker_registered(WorkerInfo::new(worker_id))
            .unwrap();

        let task = Task::new(TaskPayload::new("compute"));
        let task_id = task.id;
        scheduler.submit(task);

        let assignments = scheduler.schedule();
        assert_eq!(
            assignments.len(),
            1,
            "one task + one worker → one assignment"
        );
        assert_eq!(assignments[0].task_id, task_id);
        assert_eq!(assignments[0].worker_id, worker_id);
    }

    #[test]
    fn scheduler_does_not_assign_same_task_twice() {
        let mut scheduler = LocalScheduler::new();
        scheduler
            .worker_registered(WorkerInfo::new(WorkerId::new()))
            .unwrap();

        scheduler.submit(Task::new(TaskPayload::new("once")));

        let first = scheduler.schedule();
        let second = scheduler.schedule();

        assert_eq!(first.len(), 1, "first call: task gets assigned");
        assert!(second.is_empty(), "second call: task already assigned");
    }

    #[test]
    fn scheduler_worker_finished_marks_worker_idle() {
        let mut scheduler = LocalScheduler::new();

        let worker_id = WorkerId::new();
        scheduler
            .worker_registered(WorkerInfo::new(worker_id))
            .unwrap();

        let task = Task::new(TaskPayload::new("job"));
        let task_id = task.id;
        scheduler.submit(task);
        scheduler.schedule();

        // Worker reports success.
        let result = rivet_core::TaskResult::Success {
            task_id,
            output: vec![],
        };
        scheduler.worker_finished(result).unwrap();

        // After finishing, the worker should be eligible for a new assignment.
        scheduler.submit(Task::new(TaskPayload::new("next-job")));
        let assignments = scheduler.schedule();

        assert_eq!(
            assignments.len(),
            1,
            "worker should be idle again after reporting a result"
        );
    }
}
