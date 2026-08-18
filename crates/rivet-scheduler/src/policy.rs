use crate::TaskAssignment;
use rivet_core::{Task, WorkerId, WorkerInfo};
use std::collections::{HashMap, VecDeque};
use std::vec::Vec;

pub trait SchedulerPolicy: std::fmt::Debug {
    fn schedule(
        self: &mut Self,
        workers: &mut HashMap<WorkerId, WorkerInfo>,
        pending_tasks: &mut VecDeque<Task>,
    ) -> Vec<TaskAssignment>;
}

#[derive(Debug)]
pub enum PolicyName{
    FirstAvailablePolicyName,
    LeastLoadedPolicyName
}

#[derive(Debug, Clone)]
pub struct FirstAvailablePolicy {}

impl SchedulerPolicy for FirstAvailablePolicy {
    fn schedule(
        self: &mut Self,
        workers: &mut HashMap<WorkerId, WorkerInfo>,
        pending_tasks: &mut VecDeque<Task>,
    ) -> Vec<TaskAssignment> {
        let mut assignments: Vec<TaskAssignment> = Vec::new();

        for (_worker_id, worker) in workers.iter_mut() {
            while worker.is_available() {
                let task_opt: Option<Task> = pending_tasks.pop_front();

                if task_opt.is_some() {
                    let task = task_opt.unwrap();
                    worker.add_inflight_task();
                    assignments.push(TaskAssignment {
                        task_id: task.id,
                        worker_id: worker.id,
                        task: task,
                    });
                } else {
                    break;
                }
            }
        }
        return assignments;
    }
}

#[derive(Debug, Clone)]
pub struct LeastLoadedPolicy {}

impl SchedulerPolicy for LeastLoadedPolicy {
    fn schedule(
        self: &mut Self,
        workers: &mut HashMap<WorkerId, WorkerInfo>,
        pending_tasks: &mut VecDeque<Task>,
    ) -> Vec<TaskAssignment> {
        let mut assignments: Vec<TaskAssignment> = Vec::new();

        loop {
            let worker = workers
                .values_mut()
                .filter(|w| w.is_available())
                .min_by_key(|w| w.in_flight);
            match worker {
                Some(worker) => {
                    let task = pending_tasks.pop_front();
                    match task {
                        Some(task) => {
                            worker.add_inflight_task();
                            assignments.push(TaskAssignment {
                                task_id: task.id,
                                worker_id: worker.id,
                                task: task,
                            })
                        }
                        None => {
                            break;
                        }
                    }
                }
                None => {
                    break;
                }
            }
        }

        return assignments;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_core::TaskPayload;

    /// Build a worker map from a list of capacities.
    /// Returns the map plus the ids in the order they were created.
    fn workers(capacities: &[usize]) -> (HashMap<WorkerId, WorkerInfo>, Vec<WorkerId>) {
        let mut map = HashMap::new();
        let mut ids = Vec::new();
        for &capacity in capacities {
            let id = WorkerId::new();
            map.insert(id, WorkerInfo::new(id).with_capacity(capacity));
            ids.push(id);
        }
        (map, ids)
    }

    fn tasks(n: usize) -> VecDeque<Task> {
        (0..n)
            .map(|i| Task::new(TaskPayload::new(format!("task-{i}"))))
            .collect()
    }

    fn loads(map: &HashMap<WorkerId, WorkerInfo>, ids: &[WorkerId]) -> Vec<usize> {
        ids.iter().map(|id| map[id].in_flight).collect()
    }

    #[test]
    fn respects_capacity() {
        let (mut map, ids) = workers(&[2]);
        let mut queue = tasks(3);

        let assignments = FirstAvailablePolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 2, "capacity 2 means at most 2 assignments");
        assert_eq!(queue.len(), 1, "the third task stays pending");
        assert_eq!(map[&ids[0]].in_flight, 2);
    }

    #[test]
    fn fills_to_capacity() {
        let (mut map, ids) = workers(&[3]);
        let mut queue = tasks(3);

        let assignments = FirstAvailablePolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 3);
        assert!(
            assignments.iter().all(|a| a.worker_id == ids[0]),
            "all three should go to the only worker"
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn first_available_stops_when_queue_empties() {
        // Regression: capacity outlasts the queue. An earlier version spun
        // forever here, because popping None left the worker still available.
        let (mut map, ids) = workers(&[3]);
        let mut queue = tasks(2);

        let assignments = FirstAvailablePolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 2);
        assert_eq!(map[&ids[0]].in_flight, 2);
        assert!(queue.is_empty());
    }

    #[test]
    fn least_loaded_prefers_the_emptier_worker() {
        let (mut map, ids) = workers(&[2, 2]);
        map.get_mut(&ids[0]).unwrap().add_inflight_task(); // ids[0] starts at 1
        let mut queue = tasks(1);

        let assignments = LeastLoadedPolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 1);
        assert_eq!(
            assignments[0].worker_id, ids[1],
            "the idle worker should win over the one already holding a task"
        );
    }

    #[test]
    fn least_loaded_balances() {
        let (mut map, ids) = workers(&[2, 2, 2]);
        let mut queue = tasks(4);

        let assignments = LeastLoadedPolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 4);
        let loads = loads(&map, &ids);
        let spread = loads.iter().max().unwrap() - loads.iter().min().unwrap();
        assert!(spread <= 1, "load should be even, got {loads:?}");
    }

    #[test]
    fn least_loaded_respects_capacity() {
        let (mut map, _ids) = workers(&[1, 1]);
        let mut queue = tasks(3);

        let assignments = LeastLoadedPolicy {}.schedule(&mut map, &mut queue);

        assert_eq!(assignments.len(), 2, "two workers of capacity 1");
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn least_loaded_with_no_tasks_assigns_nothing() {
        let (mut map, _ids) = workers(&[2]);
        let mut queue = VecDeque::new();

        let assignments = LeastLoadedPolicy {}.schedule(&mut map, &mut queue);

        assert!(assignments.is_empty());
    }
}
