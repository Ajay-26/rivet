use std::collections::VecDeque;

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
    /// `waiting_on[B] == [A]` — "B is waiting on A". Answers "can this run?".
    ///
    /// Named this way rather than `dependencies`/`dependents`, which differ by
    /// two letters and mean opposite things: a swapped index would read like
    /// correct code.
    waiting_on: std::collections::HashMap<TaskId, Vec<TaskId>>,
    /// `blocks[A] == [B]` — "A blocks B". Answers "A failed, who else is doomed?".
    ///
    /// The same edges as `waiting_on`, reversed. Kept separately so the failure
    /// cascade does not have to scan every entry.
    blocks: std::collections::HashMap<TaskId, Vec<TaskId>>,
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
            waiting_on: std::collections::HashMap::new(),
            blocks: std::collections::HashMap::new(),
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
            waiting_on: std::collections::HashMap::new(),
            blocks: std::collections::HashMap::new(),
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
            waiting_on: std::collections::HashMap::new(),
            blocks: std::collections::HashMap::new(),
        }
    }
}

impl Default for LocalScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(PartialEq)]
enum ExploredStatus {
    Unexplored,
    Exploring,
    Explored,
}

impl Scheduler for LocalScheduler {
    fn submit(&mut self, mut _task: Task) -> Result<TaskId, RivetError> {
        let deps = std::mem::take(&mut _task.depends_on);
        for dep_id in deps.iter() {
            self.blocks
                .entry(dep_id.clone())
                .or_insert_with(Vec::new)
                .push(_task.id.clone());
        }
        let mut explored = std::collections::HashMap::<TaskId, ExploredStatus>::new();
        let valid = self.valid_submission(&(_task.id), &mut explored);
        if valid.is_err() {
            // Dependency, so now let's remove the entries we just added.
            // This is a roundabout way of checking for dependencies.
            // It is only helpful if we allow multiple copies of the same task with different dependencies to be submitted.
            // It is only here to add to the programming exercise.
            for dep_id in deps.iter() {
                let _ = self
                    .blocks
                    .entry(dep_id.clone())
                    .or_insert_with(Vec::new)
                    .pop();
            }
            return Err(valid.err().unwrap());
        }
        self.waiting_on.insert(_task.id, deps);
        self.pending.push_back(_task);
        Ok(self.pending.back().unwrap().id)
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
        let mut ready = VecDeque::<Task>::new();
        let mut not_ready = VecDeque::<Task>::new();
        for task in self.pending.drain(std::ops::RangeFull) {
            let items = self.waiting_on.get(&(task.id));
            match items {
                Some(items) => {
                    let mut is_ready = true;
                    for elt in items {
                        let res = self.results.get(elt);
                        match res {
                            Some(TaskResult::Success {
                                task_id: _task_id,
                                output: _output,
                            }) => {}
                            Some(TaskResult::Failure {
                                task_id: _task_id,
                                error: _error,
                            }) => {
                                is_ready = false;
                                break;
                            }
                            None => {
                                is_ready = false;
                                break;
                            }
                        }
                    }
                    if is_ready {
                        ready.push_back(task);
                    } else {
                        not_ready.push_back(task);
                    }
                }
                None => {
                    ready.push_back(task);
                }
            }
        }
        let assignments = self.policy.schedule(&mut self.workers, &mut ready);

        ready.append(&mut not_ready);
        self.pending = ready;

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
                    if *attempts < self.max_retries {
                        self.attempts.entry(task_id).and_modify(|e| {
                            *e += 1;
                        });
                        let task = assignment.task;
                        let _ = self.submit(task);

                    // Otherwise the task has failed, insert the result
                    } else {
                        self.results.insert(task_id, _result);

                        let mut doomed = std::collections::HashSet::<TaskId>::new();
                        let mut todo_list = std::vec::Vec::<TaskId>::from([task_id]);

                        while !todo_list.is_empty() {
                            let x = todo_list.pop().unwrap();
                            let blocked = self.blocks.get(&x);
                            match blocked {
                                Some(blocked) => {
                                    for elt in blocked.iter() {
                                        doomed.insert(elt.clone());
                                        todo_list.push(elt.clone());
                                    }
                                }
                                None => {}
                            }
                        }
                        self.pending.retain(|x| !doomed.contains(&(x.id)));
                        for d in doomed.drain() {
                            self.results.insert(
                                d,
                                TaskResult::Failure {
                                    task_id: d,
                                    error: format!("dependency {} failed", d),
                                },
                            );
                        }
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
                    // Requeueing a task that was already accepted cannot
                    // introduce a cycle, so this error is not reachable. Log
                    // rather than discard, in case that ever stops being true.
                    if let Err(e) = self.submit(assignment.task) {
                        eprintln!("could not requeue task {task_id}: {e}");
                    }
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

    fn valid_submission(
        self: &Self,
        starting_point: &TaskId,
        explored: &mut std::collections::HashMap<TaskId, ExploredStatus>,
    ) -> Result<(), RivetError> {
        let entry = explored
            .entry(starting_point.clone())
            .or_insert(ExploredStatus::Unexplored);
        if *entry == ExploredStatus::Exploring {
            return Err(RivetError::DependencyCycle(starting_point.clone()));
        }
        *entry = ExploredStatus::Exploring;
        let ngbs = self.blocks.get(starting_point);
        if ngbs.is_some() {
            for ngb in ngbs.unwrap() {
                let res = self.valid_submission(ngb, explored);
                if res.is_err() {
                    return res;
                }
            }
        }
        explored.insert(*starting_point, ExploredStatus::Explored);
        return Ok(());
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyName;
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

    /// `with_policy` is the only way to reach `LeastLoadedPolicy` from outside
    /// the crate, so a wrong arm in the match is invisible until load skews.
    #[test]
    fn with_policy_selects_the_named_policy() {
        for name in [
            PolicyName::FirstAvailablePolicyName,
            PolicyName::LeastLoadedPolicyName,
        ] {
            let mut scheduler = LocalScheduler::with_policy(&name);
            register(&mut scheduler, 1);
            scheduler
                .submit(Task::new(TaskPayload::new("job")))
                .expect("submit should accept this task");
            assert_eq!(
                scheduler.schedule().len(),
                1,
                "{name:?} should still dispatch a single task to a free worker"
            );
        }
    }

    #[test]
    fn least_loaded_spreads_where_first_available_stacks() {
        // One worker with 2 slots, one with 1. First-available fills the first
        // worker; least-loaded takes the emptier one second.
        let mut scheduler = LocalScheduler::with_policy(&PolicyName::LeastLoadedPolicyName);
        let big = register(&mut scheduler, 2);
        let small = register(&mut scheduler, 2);

        scheduler
            .submit(Task::new(TaskPayload::new("a")))
            .expect("submit should accept this task");
        scheduler
            .submit(Task::new(TaskPayload::new("b")))
            .expect("submit should accept this task");
        let assignments = scheduler.schedule();

        assert_eq!(assignments.len(), 2);
        let mut used: Vec<_> = assignments.iter().map(|a| a.worker_id).collect();
        used.sort_by_key(|w| w.as_u64());
        let mut expected = vec![big, small];
        expected.sort_by_key(|w| w.as_u64());
        assert_eq!(
            used, expected,
            "least-loaded must use both workers before doubling up on either"
        );
    }

    #[test]
    fn registering_the_same_worker_twice_is_rejected() {
        let mut scheduler = LocalScheduler::new();
        let id = register(&mut scheduler, 1);
        let again = scheduler.worker_registered(WorkerInfo::new(id));
        assert!(
            matches!(again, Err(RivetError::WorkerAlreadyRegistered(_))),
            "a duplicate id would silently replace the live worker's state, got {again:?}"
        );
    }

    #[test]
    fn a_result_for_an_unknown_task_is_rejected() {
        let mut scheduler = LocalScheduler::new();
        let outcome = scheduler.worker_finished(failure(TaskId::new()));
        assert!(
            matches!(outcome, Err(RivetError::TaskNotFound(_))),
            "a result nobody asked for must not be filed, got {outcome:?}"
        );
    }

    #[test]
    fn a_task_is_never_dispatched_twice() {
        let mut scheduler = LocalScheduler::new();
        register(&mut scheduler, 4);
        scheduler
            .submit(Task::new(TaskPayload::new("once")))
            .expect("submit should accept this task");

        assert_eq!(scheduler.schedule().len(), 1);
        assert!(
            scheduler.schedule().is_empty(),
            "the second schedule found the task again; dispatch must remove it \
             from pending"
        );
    }

    /// `RuntimeInner` lives behind `Arc<Mutex<..>>`, so the scheduler has to be
    /// `Send`. It is only `Send` if `Box<dyn SchedulerPolicy>` is, which needs
    /// the `+ Send` supertrait on the trait declaration.
    #[test]
    fn the_scheduler_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<LocalScheduler>();
    }

    #[test]
    fn a_failed_task_is_retried() {
        let mut scheduler = LocalScheduler::with_max_retries(4);
        register(&mut scheduler, 1);

        let id = scheduler
            .submit(Task::new(TaskPayload::new("flaky")))
            .expect("submit should accept this task");
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

        let id = scheduler
            .submit(Task::new(TaskPayload::new("flaky")))
            .expect("submit should accept this task");
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

        let id = scheduler
            .submit(Task::new(TaskPayload::new("always-fails")))
            .expect("submit should accept this task");

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

        scheduler
            .submit(Task::new(TaskPayload::new("a")))
            .expect("submit should accept this task");
        scheduler
            .submit(Task::new(TaskPayload::new("b")))
            .expect("submit should accept this task");

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

        let id = scheduler
            .submit(Task::new(TaskPayload::new("stranded")))
            .expect("submit should accept this task");
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

// ── Milestone 7 tests: task graphs ───────────────────────────────────────────

#[cfg(test)]
mod graph_tests {
    use super::*;
    use crate::policy::PolicyName;
    use rivet_core::TaskPayload;

    fn register(scheduler: &mut LocalScheduler, capacity: usize) -> WorkerId {
        let id = WorkerId::new();
        scheduler
            .worker_registered(WorkerInfo::new(id).with_capacity(capacity))
            .expect("registration should succeed");
        id
    }

    fn task(name: &str, waits_for: Vec<TaskId>) -> Task {
        Task::new(TaskPayload::new(name)).with_dependencies(waits_for)
    }

    fn success(task_id: TaskId) -> TaskResult {
        TaskResult::Success {
            task_id,
            output: Vec::new(),
        }
    }

    fn failure(task_id: TaskId) -> TaskResult {
        TaskResult::Failure {
            task_id,
            error: String::from("boom"),
        }
    }

    /// Submit a task and return its id. The id is minted by `Task::new`, so we
    /// have to read it before handing the task over.
    fn submit(scheduler: &mut LocalScheduler, t: Task) -> TaskId {
        let id = t.id;
        scheduler.submit(t).expect("submit should accept this task");
        id
    }

    // ── Eligibility ──────────────────────────────────────────────────────────

    #[test]
    fn a_task_with_no_dependencies_is_unaffected() {
        let mut scheduler = LocalScheduler::new();
        register(&mut scheduler, 4);
        let id = submit(&mut scheduler, task("lonely", Vec::new()));

        let got = scheduler.schedule();
        assert_eq!(got.len(), 1, "an empty dependency list must not block");
        assert_eq!(got[0].task_id, id);
    }

    #[test]
    fn submit_takes_the_dependency_list_off_the_task() {
        // The graph belongs to the scheduler. `Task.depends_on` only carries it
        // one hop. If this ever fails, a dependency list is crossing the socket
        // to a worker that has no use for it.
        let mut scheduler = LocalScheduler::new();
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));

        assert_eq!(
            scheduler.waiting_on.get(&b).map(|v| v.as_slice()),
            Some([a].as_slice()),
            "the scheduler must keep the edges"
        );
        let stored = scheduler
            .pending
            .iter()
            .find(|t| t.id == b)
            .expect("b is queued");
        assert!(
            stored.depends_on.is_empty(),
            "the list must be taken off the task, not copied"
        );
    }

    #[test]
    fn a_blocked_task_is_not_dispatched() {
        let mut scheduler = LocalScheduler::new();
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));

        let got = scheduler.schedule();
        assert_eq!(got.len(), 1, "only A is ready; got {got:?}");
        assert_eq!(got[0].task_id, a);
        assert_ne!(got[0].task_id, b, "B must wait for A");
    }

    #[test]
    fn a_task_runs_once_its_dependency_succeeds() {
        let mut scheduler = LocalScheduler::new();
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));

        scheduler.schedule();
        scheduler.worker_finished(success(a)).unwrap();

        let got = scheduler.schedule();
        assert_eq!(got.len(), 1, "A is done, so B should run; got {got:?}");
        assert_eq!(got[0].task_id, b);
    }

    #[test]
    fn a_failed_dependency_never_unblocks_its_dependent() {
        // A failure is still a result. Checking only "is there a result?" would
        // let B run even though A can never succeed.
        let mut scheduler = LocalScheduler::with_max_retries(1);
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        submit(&mut scheduler, task("b", vec![a]));

        scheduler.schedule();
        scheduler.worker_finished(failure(a)).unwrap();
        assert!(scheduler.results.contains_key(&a), "A failed for good");

        assert!(
            scheduler.schedule().is_empty(),
            "B waits on a task that will never succeed, so it must never run"
        );
    }

    #[test]
    fn a_diamond_runs_in_topological_order() {
        // A -> {B, C} -> D. D may only run after both B and C succeed.
        let mut scheduler = LocalScheduler::new();
        register(&mut scheduler, 8);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));
        let c = submit(&mut scheduler, task("c", vec![a]));
        let d = submit(&mut scheduler, task("d", vec![b, c]));

        assert_eq!(scheduler.schedule().len(), 1, "only A is ready");
        scheduler.worker_finished(success(a)).unwrap();

        let mut second: Vec<_> = scheduler.schedule().iter().map(|x| x.task_id).collect();
        second.sort_by_key(|i| i.as_u64());
        let mut want = vec![b, c];
        want.sort_by_key(|i| i.as_u64());
        assert_eq!(second, want, "B and C unblock together");

        scheduler.worker_finished(success(b)).unwrap();
        assert!(
            scheduler.schedule().is_empty(),
            "D needs both B and C, not just one"
        );

        scheduler.worker_finished(success(c)).unwrap();
        let last = scheduler.schedule();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].task_id, d, "D runs last");
    }

    #[test]
    fn both_policies_respect_dependencies() {
        // The gate belongs to the scheduler, not to a policy. If it ever moves
        // into one, the other policy loses it and this test fails.
        for name in [
            PolicyName::FirstAvailablePolicyName,
            PolicyName::LeastLoadedPolicyName,
        ] {
            let mut scheduler = LocalScheduler::with_policy(&name);
            register(&mut scheduler, 4);
            let a = submit(&mut scheduler, task("a", Vec::new()));
            submit(&mut scheduler, task("b", vec![a]));

            let got = scheduler.schedule();
            assert_eq!(got.len(), 1, "{name:?} dispatched a blocked task");
            assert_eq!(got[0].task_id, a, "{name:?} picked the wrong task");
        }
    }

    // ── Cycles ───────────────────────────────────────────────────────────────

    #[test]
    fn a_self_dependency_is_rejected() {
        let mut scheduler = LocalScheduler::new();
        let mut t = task("loop", Vec::new());
        t.depends_on = vec![t.id];

        let outcome = scheduler.submit(t);
        assert!(
            matches!(outcome, Err(RivetError::DependencyCycle(_))),
            "a task depending on itself must be refused; got {outcome:?}"
        );
    }

    #[test]
    fn a_cycle_is_rejected_at_submit() {
        // A waits on C, B waits on A, C waits on B. The third submit closes it.
        let mut scheduler = LocalScheduler::new();
        let (a, b, c) = (TaskId::new(), TaskId::new(), TaskId::new());

        let mut ta = task("a", vec![c]);
        ta.id = a;
        let mut tb = task("b", vec![a]);
        tb.id = b;
        let mut tc = task("c", vec![b]);
        tc.id = c;

        assert!(
            scheduler.submit(ta).is_ok(),
            "naming a task that is not submitted yet is allowed"
        );
        assert!(scheduler.submit(tb).is_ok());

        let outcome = scheduler.submit(tc);
        assert!(
            matches!(outcome, Err(RivetError::DependencyCycle(_))),
            "the third submit closes the loop; got {outcome:?}"
        );
        assert!(
            !scheduler.pending.iter().any(|t| t.id == c),
            "a rejected task must not be stored"
        );
    }

    #[test]
    fn a_diamond_is_accepted() {
        // The false positive a single visited set gives you. Submitting A last
        // makes the walk reach D by two different paths.
        let mut scheduler = LocalScheduler::new();
        let (a, b, c, d) = (TaskId::new(), TaskId::new(), TaskId::new(), TaskId::new());

        let mut tb = task("b", vec![a]);
        tb.id = b;
        let mut tc = task("c", vec![a]);
        tc.id = c;
        let mut td = task("d", vec![b, c]);
        td.id = d;
        let mut ta = task("a", Vec::new());
        ta.id = a;

        scheduler.submit(tb).unwrap();
        scheduler.submit(tc).unwrap();
        scheduler.submit(td).unwrap();
        assert!(
            scheduler.submit(ta).is_ok(),
            "reaching D twice is a diamond, not a cycle. Two states are needed: \
             on the current path, and already finished."
        );
    }

    #[test]
    fn a_long_chain_is_accepted() {
        let mut scheduler = LocalScheduler::new();
        let mut previous = submit(&mut scheduler, task("step-0", Vec::new()));
        for i in 1..20 {
            previous = submit(&mut scheduler, task(&format!("step-{i}"), vec![previous]));
        }
    }

    // ── Failure cascade ──────────────────────────────────────────────────────

    #[test]
    fn a_permanently_failed_dependency_fails_what_it_blocks() {
        let mut scheduler = LocalScheduler::with_max_retries(1);
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));

        scheduler.schedule();
        scheduler.worker_finished(failure(a)).unwrap();

        let b_result = scheduler
            .results
            .get(&b)
            .expect("B can never run, so it needs a result of its own");
        assert!(!b_result.is_success(), "B's result must be a failure");
        assert!(
            !scheduler.pending.iter().any(|t| t.id == b),
            "B must also leave the queue, or it sits there for ever"
        );
    }

    #[test]
    fn a_failure_cascades_through_a_chain() {
        // A <- B <- C. A fails, so both B and C are doomed.
        let mut scheduler = LocalScheduler::with_max_retries(1);
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));
        let c = submit(&mut scheduler, task("c", vec![b]));

        scheduler.schedule();
        scheduler.worker_finished(failure(a)).unwrap();

        assert!(scheduler.results.contains_key(&b), "B waits on A");
        assert!(
            scheduler.results.contains_key(&c),
            "C waits on B, so the walk must go all the way down the chain"
        );
        assert!(scheduler.pending.is_empty(), "nothing is left to run");
    }

    #[test]
    fn a_dependency_still_retrying_keeps_the_dependent_blocked() {
        // A has attempts left, so it has not failed. Nothing cascades yet.
        let mut scheduler = LocalScheduler::with_max_retries(4);
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", vec![a]));

        scheduler.schedule();
        scheduler.worker_finished(failure(a)).unwrap();

        assert!(
            !scheduler.results.contains_key(&a),
            "A still has attempts left"
        );
        assert!(
            !scheduler.results.contains_key(&b),
            "so B must be neither dispatched nor failed"
        );

        let got = scheduler.schedule();
        assert_eq!(got.len(), 1, "A is retried, B stays blocked; got {got:?}");
        assert_eq!(got[0].task_id, a);
    }

    #[test]
    fn a_task_blocked_by_two_failures_is_only_failed_once() {
        let mut scheduler = LocalScheduler::with_max_retries(1);
        register(&mut scheduler, 4);
        let a = submit(&mut scheduler, task("a", Vec::new()));
        let b = submit(&mut scheduler, task("b", Vec::new()));
        let c = submit(&mut scheduler, task("c", vec![a, b]));

        scheduler.schedule();
        scheduler.worker_finished(failure(a)).unwrap();
        scheduler.worker_finished(failure(b)).unwrap();

        assert!(scheduler.results.contains_key(&c), "C is doomed either way");
        assert!(scheduler.pending.is_empty());
    }
}
