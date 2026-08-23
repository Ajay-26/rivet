use crate::LocalClient;
use rivet_core::{TaskResult, WorkerId, WorkerInfo, WorkerStatus};
use rivet_scheduler::{LocalScheduler, Scheduler};
use rivet_worker::{spawn, WorkerHandle};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

#[derive(Debug)]
pub(crate) struct RuntimeInner {
    pub(crate) scheduler: LocalScheduler,
    workers: HashMap<WorkerId, WorkerHandle>,
    results_receiver: mpsc::Receiver<TaskResult>,
}

#[derive(Debug)]
pub struct LocalRuntime {
    inner: Arc<Mutex<RuntimeInner>>,
}

impl LocalRuntime {
    pub fn new(worker_count: usize, capacity: usize) -> Self {
        let scheduler = LocalScheduler::new();
        let (results_sender, results_receiver) = mpsc::channel::<TaskResult>();
        let workers = HashMap::<WorkerId, WorkerHandle>::new();

        let mut inner = RuntimeInner {
            scheduler,
            workers,
            results_receiver,
        };

        for _ in 0..worker_count {
            let worker = spawn(capacity, results_sender.clone());
            let _ = inner.scheduler.worker_registered(WorkerInfo {
                id: worker.get_id(),
                status: WorkerStatus::Online,
                address: None,
                capacity,
                in_flight: 0,
            });
            inner.workers.insert(worker.get_id(), worker);
        }
        drop(results_sender);
        LocalRuntime {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn client(&self) -> LocalClient {
        LocalClient {
            runtime: self.inner.clone(),
        }
    }

    pub fn tick(&mut self) {
        let inner_runtime = self.inner.lock();
        match inner_runtime {
            Ok(mut inner_runtime) => {
                let assignments = inner_runtime.scheduler.schedule();

                // Iterate through tasks for each handle
                for elt in assignments.into_iter() {
                    let worker_handle = inner_runtime.workers.get(&elt.worker_id);
                    match worker_handle {
                        Some(worker_handle) => {
                            let _ = worker_handle.send(elt.task);
                        }
                        None => {
                            panic!("Could not find worker!");
                        }
                    }
                }

                // Drain results receiver and collect results
                loop {
                    let result = inner_runtime.results_receiver.try_recv();
                    match result {
                        Ok(result) => {
                            let _ = inner_runtime.scheduler.worker_finished(result);
                        }
                        Err(mpsc::TryRecvError::Empty) => {
                            break;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            break;
                        }
                    }
                }
            }
            Err(_) => {
                panic!("Poisoned mutex!");
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Client;
    use rivet_core::{TaskId, TaskPayload};
    use std::time::{Duration, Instant};

    /// Tick in a loop until `want` results have landed, or the bound is hit.
    ///
    /// Bounded on purpose: a stranded task must report a failure, not hang the
    /// suite. 200 x 10ms = 2s, and a task takes ~100ms.
    fn tick_until(runtime: &mut LocalRuntime, want: usize) -> usize {
        for _ in 0..200 {
            runtime.tick();
            let done = runtime.inner.lock().unwrap().scheduler.get_results().len();
            if done >= want {
                return done;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        runtime.inner.lock().unwrap().scheduler.get_results().len()
    }

    fn submit_n(runtime: &LocalRuntime, n: usize) -> Vec<TaskId> {
        let mut client = runtime.client();
        (0..n)
            .map(|i| {
                client
                    .submit(TaskPayload::new(&format!("job-{i}")))
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn pool_ids_match_registered_worker_ids() {
        let runtime = LocalRuntime::new(3, 1);
        submit_n(&runtime, 3);

        let mut inner = runtime.inner.lock().unwrap();
        assert_eq!(
            inner.workers.len(),
            3,
            "new(3, 1) should leave 3 handles in the pool. If this is 0, `new` \
             filled a local map that was dropped at the end of the function."
        );

        let assignments = inner.scheduler.schedule();
        assert_eq!(
            assignments.len(),
            3,
            "3 workers of capacity 1 should take 3 tasks. If this is 0, the \
             workers were never registered with the scheduler."
        );
        for a in &assignments {
            assert!(
                inner.workers.contains_key(&a.worker_id),
                "the scheduler assigned work to {:?}, which is not in the pool. \
                 spawn() and worker_registered() must use the same WorkerId.",
                a.worker_id
            );
        }
    }

    #[test]
    fn registered_capacity_matches_the_thread_count() {
        let runtime = LocalRuntime::new(1, 3);
        submit_n(&runtime, 3);

        let mut inner = runtime.inner.lock().unwrap();
        let assignments = inner.scheduler.schedule();
        assert_eq!(
            assignments.len(),
            3,
            "one worker spawned with capacity 3 has 3 threads, so the scheduler \
             should be willing to give it 3 tasks. Register the WorkerInfo with \
             .with_capacity(capacity)."
        );
    }

    #[test]
    fn tick_on_an_idle_runtime_does_nothing() {
        let mut runtime = LocalRuntime::new(1, 1);

        let start = Instant::now();
        runtime.tick();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(50),
            "tick took {elapsed:?} with nothing submitted; the result drain must \
             use try_recv, not recv."
        );
        assert!(
            runtime
                .inner
                .lock()
                .unwrap()
                .scheduler
                .get_results()
                .is_empty(),
            "no task was submitted, so there should be no results"
        );
    }

    #[test]
    fn tick_dispatches_and_collects_one_task() {
        let mut runtime = LocalRuntime::new(1, 1);
        let ids = submit_n(&runtime, 1);

        assert_eq!(tick_until(&mut runtime, 1), 1, "the task should complete");

        let inner = runtime.inner.lock().unwrap();
        let result = inner
            .scheduler
            .get_results()
            .get(&ids[0])
            .expect("the result should be filed under the submitted id");
        assert!(result.is_success());
    }

    #[test]
    fn capacity_is_returned_after_completion() {
        // One worker, one slot, two tasks. The second can only run once the
        // first has released the slot.
        let mut runtime = LocalRuntime::new(1, 1);
        let ids = submit_n(&runtime, 2);

        assert_eq!(
            tick_until(&mut runtime, 2),
            2,
            "the second task was never dispatched. worker_finished() is what \
             decrements in_flight; without it the pool wedges after one round."
        );

        let inner = runtime.inner.lock().unwrap();
        for id in ids {
            assert!(inner.scheduler.get_results().contains_key(&id));
        }
    }

    #[test]
    fn backlog_drains_over_several_ticks() {
        let mut runtime = LocalRuntime::new(2, 2);
        let ids = submit_n(&runtime, 10);

        assert_eq!(
            tick_until(&mut runtime, 10),
            10,
            "10 tasks over 4 slots should drain; it did not terminate"
        );

        let inner = runtime.inner.lock().unwrap();
        for id in ids {
            assert!(
                inner.scheduler.get_results().contains_key(&id),
                "task {id:?} never produced a result"
            );
        }
    }
}
