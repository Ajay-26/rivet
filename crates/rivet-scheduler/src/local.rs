use crate::policy::{FirstAvailablePolicy, LeastLoadedPolicy, PolicyName, SchedulerPolicy};
use crate::{Scheduler, TaskAssignment};
use rivet_core::{RivetError, Task, TaskId, TaskResult, WorkerId, WorkerInfo, WorkerStatus};

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
    assigned: std::collections::HashMap<TaskId, TaskAssignment>,
    results: std::collections::HashMap<TaskId, TaskResult>,
    policy: Box<dyn SchedulerPolicy>,
    max_retries: u32,
    attempts: std::collections::HashMap<TaskId, u32>,
}

impl LocalScheduler {
    pub fn new() -> Self {
        LocalScheduler {
            pending: std::collections::VecDeque::new(),
            workers: std::collections::HashMap::new(),
            assigned: std::collections::HashMap::new(),
            results: std::collections::HashMap::new(),
            policy: Box::new(FirstAvailablePolicy {}),
            max_retries: 4,
            attempts: std::collections::HashMap::new(),
        }
    }

    pub fn with_policy(policy_name: &PolicyName) -> Self {
        LocalScheduler {
            pending: std::collections::VecDeque::new(),
            workers: std::collections::HashMap::new(),
            assigned: std::collections::HashMap::new(),
            results: std::collections::HashMap::new(),
            policy: match policy_name {
                PolicyName::FirstAvailablePolicyName => Box::new(FirstAvailablePolicy {}),
                PolicyName::LeastLoadedPolicyName => Box::new(LeastLoadedPolicy {}),
            },
            max_retries: 4,
            attempts: std::collections::HashMap::new(),
        }
    }

    pub fn with_max_retries(max_retries: u32) -> Self {
        LocalScheduler {
            pending: std::collections::VecDeque::new(),
            workers: std::collections::HashMap::new(),
            assigned: std::collections::HashMap::new(),
            results: std::collections::HashMap::new(),
            policy: Box::new(FirstAvailablePolicy {}),
            max_retries: max_retries,
            attempts: std::collections::HashMap::new(),
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
        self.pending.back().unwrap().id
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
            self.assigned.insert(a.task_id, a.clone());
        }
        assignments
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
        let task_assignment = self.assigned.remove(&task_id);

        if task_assignment.is_some() {
            let assignment = task_assignment.unwrap();
            let worker: &mut WorkerInfo = self.workers.get_mut(&assignment.worker_id).unwrap();
            worker.remove_inflight_task();

            match _result {
                TaskResult::Success { task_id, output: _ } => {
                    // Store the result somewhere the client can retrieve it.
                    self.results.insert(task_id, _result);
                }
                TaskResult::Failure { task_id, error: _ } => {
                    // Store the result somewhere the client can retrieve it.
                    let attempts = self.attempts.entry(task_id).or_insert(1);

                    // Launch another attempt of the task, if valid
                    if *attempts + 1 < self.max_retries {
                        self.attempts.entry(task_id).and_modify(|e| {
                            *e += 1;
                        });
                        let task = assignment.task;
                        let _ = self.submit(task);

                    // Otherwise the task has failed, insert the result
                    } else {
                        self.results.insert(task_id, _result);
                    }
                }
            }
            Ok(())
        } else {
            //   Return Err(RivetError::TaskNotFound(...)) if the task ID is unknown.
            Err(RivetError::TaskNotFound(task_id))
        }
    }

    fn worker_offline(&mut self, id: WorkerId) -> Result<(), RivetError> {
        let worker = self.workers.get_mut(&id);
        match worker {
            Some(worker) => {
                worker.status = WorkerStatus::Offline;
                let stranded: Vec<TaskId> = self
                    .assigned
                    .iter()
                    .filter(|(_, a)| a.worker_id == id)
                    .map(|(task_id, _)| *task_id)
                    .collect();

                for task_id in stranded.iter() {
                    let assignment = self.assigned.remove(task_id).unwrap();
                    self.submit(assignment.task);
                }
                Ok(())
            }
            None => Err(RivetError::WorkerNotFound(id)),
        }
    }
}

impl LocalScheduler {
    pub fn get_results(&self) -> &std::collections::HashMap<TaskId, TaskResult> {
        &self.results
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::{TaskPayload, WorkerStatus};

    fn register(scheduler: &mut LocalScheduler, capacity: usize) -> WorkerId {
        let id = WorkerId::new();
        scheduler
            .worker_registered(WorkerInfo::new(id).with_capacity(capacity))
            .expect("registration should succeed");
        id
    }

    fn failure(task_id: TaskId) -> TaskResult {
        TaskResult::Failure {
            task_id,
            error: String::from("boom"),
        }
    }

    #[test]
    fn a_failed_task_is_retried() {
        let mut scheduler = LocalScheduler::with_max_retries(4);
        register(&mut scheduler, 1);

        let id = scheduler.submit(Task::new(TaskPayload::new("flaky")));
        assert_eq!(scheduler.schedule().len(), 1, "the task should dispatch");
        assert!(scheduler.pending.is_empty(), "dispatch empties pending");

        scheduler
            .worker_finished(failure(id))
            .expect("a known task id should be accepted");

        assert_eq!(
            scheduler.pending.len(),
            1,
            "a failure below the retry limit must go back on the queue"
        );
        assert_eq!(
            scheduler.pending[0].id, id,
            "the retry must keep the original task id, or the client loses it"
        );
        assert!(
            !scheduler.results.contains_key(&id),
            "a task that will be retried is not finished, so it has no result yet"
        );
    }

    #[test]
    fn a_retry_releases_the_workers_slot() {
        let mut scheduler = LocalScheduler::with_max_retries(4);
        let worker = register(&mut scheduler, 1);

        let id = scheduler.submit(Task::new(TaskPayload::new("flaky")));
        scheduler.schedule();
        assert_eq!(scheduler.workers[&worker].in_flight, 1);

        scheduler.worker_finished(failure(id)).unwrap();

        assert_eq!(
            scheduler.workers[&worker].in_flight, 0,
            "worker_finished must release the slot on failure as well as success, \
             or the retry can never be dispatched"
        );
        assert_eq!(
            scheduler.schedule().len(),
            1,
            "the retry should now be dispatchable"
        );
    }

    #[test]
    fn retries_stop_at_the_limit() {
        let mut scheduler = LocalScheduler::with_max_retries(3);
        register(&mut scheduler, 1);

        let id = scheduler.submit(Task::new(TaskPayload::new("always-fails")));

        // Keep failing it until the scheduler stops handing it back out.
        let mut dispatches = 0;
        for _ in 0..20 {
            if scheduler.schedule().is_empty() {
                break;
            }
            dispatches += 1;
            scheduler.worker_finished(failure(id)).unwrap();
        }

        assert!(
            dispatches < 20,
            "the task was still being retried after 20 attempts; max_retries is \
             not bounding the loop"
        );
        assert!(
            scheduler.pending.is_empty(),
            "once the limit is reached the task must not be requeued"
        );
        let result = scheduler
            .results
            .get(&id)
            .expect("the final failure must be stored as the task's result");
        assert!(
            !result.is_success(),
            "the stored result should be the failure"
        );
    }

    #[test]
    fn an_offline_worker_gets_no_assignments() {
        let mut scheduler = LocalScheduler::new();
        let dead = register(&mut scheduler, 1);
        let alive = register(&mut scheduler, 1);

        scheduler.worker_offline(dead).expect("dead is registered");

        scheduler.submit(Task::new(TaskPayload::new("a")));
        scheduler.submit(Task::new(TaskPayload::new("b")));

        let assignments = scheduler.schedule();
        assert_eq!(
            assignments.len(),
            1,
            "only the surviving worker has a slot, so only one task dispatches"
        );
        assert_eq!(
            assignments[0].worker_id, alive,
            "an Offline worker must never be chosen"
        );
        assert_eq!(
            scheduler.workers[&dead].status,
            WorkerStatus::Offline,
            "worker_offline must actually set the status"
        );
    }

    #[test]
    fn going_offline_requeues_that_workers_tasks() {
        let mut scheduler = LocalScheduler::new();
        let dead = register(&mut scheduler, 1);

        let id = scheduler.submit(Task::new(TaskPayload::new("stranded")));
        scheduler.schedule();
        assert!(scheduler.pending.is_empty(), "the task is now in flight");

        scheduler.worker_offline(dead).unwrap();

        assert_eq!(
            scheduler.pending.len(),
            1,
            "a task in flight on a dead worker will never report, so it must be \
             requeued from the scheduler's own copy"
        );
        assert_eq!(scheduler.pending[0].id, id);
        assert!(
            !scheduler.assigned.contains_key(&id),
            "the task is no longer assigned to the dead worker"
        );
    }

    #[test]
    fn worker_offline_rejects_an_unknown_worker() {
        let mut scheduler = LocalScheduler::new();
        let result = scheduler.worker_offline(WorkerId::new());
        assert!(
            matches!(result, Err(RivetError::WorkerNotFound(_))),
            "marking an unregistered worker offline should be an error, got {result:?}"
        );
    }
}
