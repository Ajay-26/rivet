use rivet_core::{RivetError, Task, TaskResult, WorkerId, WorkerInfo};
use crate::Worker;

/// A worker that runs tasks in the current process.
///
/// Start here for Milestone 1. For Milestone 2, you will likely want to spawn
/// OS threads (or async tasks) inside `execute` so multiple workers can run
/// concurrently.
///
/// TODO (Milestone 1): Implement `execute`.
/// TODO (Milestone 2): Spawn a thread per task; return a handle rather than blocking.
/// TODO (Milestone 4): Accept tasks over a channel from the scheduler instead
///                     of having `execute` called directly.
#[derive(Debug)]
pub struct LocalWorker {
    info: WorkerInfo,
}

impl LocalWorker {
    pub fn new() -> Self {
        LocalWorker {
            info: WorkerInfo::new(WorkerId::new()),
        }
    }
}

impl Default for LocalWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl Worker for LocalWorker {
    fn info(&self) -> &WorkerInfo {
        &self.info
    }

    fn execute(&mut self, _task: Task) -> Result<TaskResult, RivetError> {
        // TODO (Milestone 1):
        //   1. Set self.info.status to WorkerStatus::Busy.
        //   2. Run the task. For now, any successful return is fine.
        //      The `_task.payload` tells you what to do — how you interpret it
        //      is up to you (a registry of named functions, dynamic dispatch, etc.)
        //   3. Set self.info.status back to WorkerStatus::Idle.
        //   4. Return TaskResult::Success { task_id: task.id, output: ... }.
        //
        // Think about:
        //   - What should happen if the task panics? (hint: std::panic::catch_unwind)
        //   - How do you pass the output bytes back? What do they represent?
        todo!("execute the task and return a TaskResult")
    }
}
