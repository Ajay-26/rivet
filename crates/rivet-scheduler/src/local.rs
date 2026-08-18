use crate::policy::{FirstAvailablePolicy, SchedulerPolicy, PolicyName, LeastLoadedPolicy};
use crate::{Scheduler, TaskAssignment};
use rivet_core::{RivetError, Task, TaskId, TaskResult, WorkerId, WorkerInfo};

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
    assigned: std::collections::HashMap<TaskId, WorkerId>,
    results: std::collections::HashMap<TaskId, TaskResult>,
    policy: Box<dyn SchedulerPolicy>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        return LocalScheduler {
            pending: std::collections::VecDeque::new(),
            workers: std::collections::HashMap::new(),
            assigned: std::collections::HashMap::new(),
            results: std::collections::HashMap::new(),
            policy: Box::new(FirstAvailablePolicy {}),
        };
    }

    pub fn with_policy(policy_name: &PolicyName) -> Self {
        return LocalScheduler {
            pending: std::collections::VecDeque::new(),
            workers: std::collections::HashMap::new(),
            assigned: std::collections::HashMap::new(),
            results: std::collections::HashMap::new(),
            policy: match policy_name {
                PolicyName::FirstAvailablePolicyName => Box::new(FirstAvailablePolicy {}),
                PolicyName::LeastLoadedPolicyName => Box::new(LeastLoadedPolicy {})
            }
        };
    }
}

impl Default for LocalScheduler {
    fn default() -> Self {
        return Self::new();
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
        let assignments = self.policy.schedule(&mut self.workers, &mut self.pending);

        for a in assignments.iter() {
            self.assigned.insert(a.task_id, a.worker_id);
        }
        return assignments;
    }

    fn worker_registered(&mut self, _worker: WorkerInfo) -> Result<(), RivetError> {
        if self.workers.contains_key(&_worker.id) {
            return Err(RivetError::WorkerAlreadyRegistered(_worker.id));
        }
        self.workers.insert(_worker.id, _worker);
        Ok(())
    }

    fn worker_finished(&mut self, _result: TaskResult) -> Result<(), RivetError> {
        //   Look up which task just finished (use `result.task_id()`).
        let task_id: TaskId = _result.task_id();

        //   Find which worker was running it.
        let worker_id: Option<&WorkerId> = self.assigned.get(&task_id);

        if worker_id.is_some() {
            let worker: &mut WorkerInfo = self.workers.get_mut(worker_id.unwrap()).unwrap();
            worker.remove_inflight_task();

            // Store the result somewhere the client can retrieve it.
            self.results.insert(task_id, _result);

            return Ok(());
        } else {
            //   Return Err(RivetError::TaskNotFound(...)) if the task ID is unknown.
            return Err(RivetError::TaskNotFound(task_id));
        }
    }
}

impl LocalScheduler {
    pub fn get_results(self: &Self) -> &std::collections::HashMap<TaskId, TaskResult> {
        &self.results
    }
}
