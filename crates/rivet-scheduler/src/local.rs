use rivet_core::{RivetError, Task, TaskId, TaskResult, WorkerInfo};
use crate::{Scheduler, TaskAssignment};

/// A single-process scheduler — all state lives in memory, no networking.
///
/// This is the first implementation you will write. Start here for Milestone 1.
///
/// Suggested fields (you may choose different names or types):
///
/// ```text
/// pending:   a queue of tasks waiting to be assigned
/// workers:   a map from WorkerId to WorkerInfo
/// assigned:  a map from TaskId to WorkerId (tasks currently running)
/// ```
///
/// Pick the right standard-library collection for each. Think about:
///   - Which collections let you remove from the front efficiently?
///   - How do you look up a worker by ID?
///   - What's the difference between `HashMap` and `BTreeMap`?
#[derive(Debug)]
pub struct LocalScheduler {
    // TODO: Add your fields here.
    //
    // Example to get started:
    //   pending: std::collections::VecDeque<Task>,
    //   workers: std::collections::HashMap<WorkerId, WorkerInfo>,
    //   assigned: std::collections::HashMap<TaskId, WorkerId>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        LocalScheduler {
            // TODO: Initialise your fields.
        }
    }
}

impl Default for LocalScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for LocalScheduler {
    fn submit(&mut self, _task: Task) -> TaskId {
        // TODO (Milestone 1):
        //   1. Store the task in your pending queue.
        //   2. Return `task.id`.
        todo!("store the task and return its ID")
    }

    fn schedule(&mut self) -> Vec<TaskAssignment> {
        // TODO (Milestone 1):
        //   For each idle worker, if there is a pending task, pair them:
        //     - Remove the task from the pending queue.
        //     - Mark the worker as Busy.
        //     - Record the assignment so you know which worker has which task.
        //     - Push a TaskAssignment into the result Vec.
        //
        // TODO (Milestone 3): Replace this greedy round-robin with a real policy.
        todo!("match pending tasks to idle workers")
    }

    fn worker_registered(&mut self, _worker: WorkerInfo) -> Result<(), RivetError> {
        // TODO (Milestone 1):
        //   Insert the worker into your workers map.
        //   Return Err(RivetError::WorkerAlreadyRegistered(...)) if it is a duplicate.
        todo!("store the new worker")
    }

    fn worker_finished(&mut self, _result: TaskResult) -> Result<(), RivetError> {
        // TODO (Milestone 1):
        //   Look up which task just finished (use `result.task_id()`).
        //   Find which worker was running it.
        //   Mark that worker Idle again.
        //   Store the result somewhere the client can retrieve it.
        //   Return Err(RivetError::TaskNotFound(...)) if the task ID is unknown.
        todo!("update task and worker state from the result")
    }
}
