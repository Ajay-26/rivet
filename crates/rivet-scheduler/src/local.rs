use crate::{Scheduler, TaskAssignment};
use rivet_core::{RivetError, Task, TaskId, TaskResult, WorkerId, WorkerInfo};
use std::collections::HashMap;
use std::collections::VecDeque;
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
    pending: std::collections::VecDeque<Task>,
    workers: std::collections::HashMap<WorkerId, WorkerInfo>,
    // Example to get started:
    //   assigned: std::collections::HashMap<TaskId, WorkerId>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        LocalScheduler {
            pending: VecDeque::new(),
            workers: HashMap::new(),
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
        self.pending.push_back(_task);
        return self.pending.back().unwrap().id;
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
        if self.workers.contains_key(&_worker.id) {
            return Err(RivetError::WorkerAlreadyRegistered(_worker.id));
        }
        self.workers.insert(_worker.id, _worker);
        Ok(())
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
