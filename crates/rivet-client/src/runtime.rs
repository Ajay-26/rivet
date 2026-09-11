use crate::LocalClient;
use rivet_core::{TaskResult, WorkerId, WorkerInfo, WorkerStatus};
use rivet_scheduler::{LocalScheduler, Scheduler};
use rivet_worker::{spawn, RemoteWorkerHandle, WorkerTransport};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::vec::Vec;

#[derive(Debug)]
pub(crate) struct RuntimeInner {
    pub(crate) scheduler: LocalScheduler,
    workers: HashMap<WorkerId, Box<dyn WorkerTransport>>,
    results_receiver: mpsc::Receiver<TaskResult>,
}

/// Owns the scheduler and the worker pool.
///
/// The pool holds `Box<dyn WorkerTransport>`, so in-process workers and worker
/// processes sit in the same map and `tick` cannot tell them apart.
#[derive(Debug)]
pub struct LocalRuntime {
    inner: Arc<Mutex<RuntimeInner>>,
}

impl LocalRuntime {
    pub fn new(worker_count: usize, capacity: usize) -> Self {
        let scheduler = LocalScheduler::new();
        let (results_sender, results_receiver) = mpsc::channel::<TaskResult>();
        let workers = HashMap::<WorkerId, Box<dyn WorkerTransport>>::new();

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
            inner.workers.insert(worker.get_id(), Box::new(worker));
        }
        drop(results_sender);
        LocalRuntime {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Connect to worker processes that are already listening.
    ///
    /// The mirror of `new`: instead of spawning threads, it opens a socket to
    /// each address. Everything after the connection is set up is identical,
    /// which is the whole point of Milestone 4's split.
    ///
    /// TODO (Milestone 6, Step 4):
    ///   1. Make the results channel exactly as `new` does.
    ///   2. For each address: mint a `WorkerId` **here** — `WorkerId::new()` is
    ///      a per-process counter, so a worker that names itself collides with
    ///      every other worker process.
    ///   3. `RemoteWorkerHandle::connect(addr, id, results_tx.clone())`.
    ///   4. Register `WorkerInfo::new(id).with_capacity(handle.capacity())`
    ///      with the scheduler — the capacity comes from the worker's `Hello`,
    ///      not from a guess — and `.with_address(addr)`.
    ///   5. Box the handle into the pool under the same id.
    ///   6. Drop the runtime's own copy of the sender, same as `new`.
    ///
    /// One address failing to connect: decide whether that is fatal or whether
    /// the runtime should carry on with the workers it did reach. Say which in
    /// a comment. A cluster that refuses to start because one machine is down
    /// is usually the wrong answer.
    pub fn with_remote_workers(_addrs: &[std::net::SocketAddr]) -> std::io::Result<Self> {
        // todo!("Milestone 6, Step 4: connect to worker processes")
        let scheduler = LocalScheduler::new();
        let (results_sender, results_receiver) = mpsc::channel::<TaskResult>();
        let workers = HashMap::<WorkerId, Box<dyn WorkerTransport>>::new();

        let mut inner = RuntimeInner {
            scheduler,
            workers,
            results_receiver,
        };

        for addr in _addrs {
            let worker_id = WorkerId::new();
            let handle = RemoteWorkerHandle::connect(*addr, worker_id, results_sender.clone())?;
            let _res = inner.scheduler.worker_registered(WorkerInfo {
                id: WorkerId::with_id(worker_id.as_u64()),
                status: WorkerStatus::Online,
                address: Some(*addr),
                capacity: handle.capacity(),
                in_flight: 0,
            });
            if _res.is_err() {
                println!("Error registering worker");
            }
            inner.workers.insert(worker_id, Box::new(handle));
        }
        drop(results_sender);
        return Ok(LocalRuntime {
            inner: Arc::new(Mutex::new(inner)),
        });
    }

    /// Stop the workers and collect the last results.
    ///
    /// Safe to call more than once. `Drop` calls it for you, so most callers
    /// never need to.
    ///
    /// The order matters. Clearing the pool runs each handle's own `Drop`: an
    /// in-process handle closes its inbox and joins its threads, and a remote
    /// handle shuts its socket and joins its reader. Both of those finish the
    /// work already in flight and send the results. Only then is it worth
    /// draining the channel, because only then is everything in it.
    pub fn shutdown(&self) {
        // A panic elsewhere must not stop shutdown, so take the lock back out of
        // a poisoned mutex rather than giving up. Giving up here would leak
        // worker processes.
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Dropping the handles blocks until in-flight work is done.
        inner.workers.clear();

        // Now every result that will ever arrive is already in the channel.
        while let Ok(result) = inner.results_receiver.try_recv() {
            if let Err(e) = inner.scheduler.worker_finished(result) {
                eprintln!("rivet: result arrived for an unknown task: {e}");
            }
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
                let mut worker_id_cancel = Vec::<WorkerId>::new();
                for (worker_id, worker_handle) in inner_runtime.workers.iter() {
                    if !(worker_handle.is_alive()) {
                        worker_id_cancel.push(worker_id.clone());
                    }
                }

                for id in worker_id_cancel.drain(..) {
                    let _ = inner_runtime.scheduler.worker_offline(id);
                }

                let assignments = inner_runtime.scheduler.schedule();

                // Iterate through tasks for each handle
                for elt in assignments.into_iter() {
                    println!("Task assignment is {:?}", &elt);
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
    fn shutdown_waits_for_work_already_in_flight() {
        let mut runtime = LocalRuntime::new(1, 1);
        let ids = submit_n(&runtime, 1);

        runtime.tick(); // hand the task out
        std::thread::sleep(Duration::from_millis(20)); // let the worker pick it up

        let start = Instant::now();
        runtime.shutdown();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(50),
            "shutdown returned in {elapsed:?}, so it did not wait for the worker. \
             Clearing the pool must join the threads, not detach them."
        );
        assert!(
            runtime
                .inner
                .lock()
                .unwrap()
                .scheduler
                .get_results()
                .contains_key(&ids[0]),
            "shutdown must drain the channel after joining, or the last result \
             is thrown away"
        );
    }

    #[test]
    fn shutdown_can_be_called_twice() {
        // `Drop` calls it too, so a caller who calls it by hand would otherwise
        // shut down twice.
        let runtime = LocalRuntime::new(2, 1);
        runtime.shutdown();
        runtime.shutdown();
    }

    #[test]
    fn dropping_the_runtime_shuts_the_pool_down() {
        let mut runtime = LocalRuntime::new(1, 1);
        submit_n(&runtime, 1);
        runtime.tick();
        std::thread::sleep(Duration::from_millis(20));

        let start = Instant::now();
        drop(runtime);
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(50),
            "drop returned in {elapsed:?}. LocalRuntime needs a Drop impl, or \
             worker threads outlive the runtime that can no longer tick them."
        );
    }

    #[test]
    fn a_client_still_works_after_the_runtime_is_dropped() {
        // Clients hold a clone of the same Arc, so the state survives. The
        // workers do not, and that is on purpose: nothing can tick any more.
        let mut runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();
        let id = client.submit(TaskPayload::new("before")).unwrap();
        assert_eq!(tick_until(&mut runtime, 1), 1);

        drop(runtime);

        assert!(
            client.get_result(id).expect("no error").is_some(),
            "a result recorded before shutdown must still be readable"
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
    fn the_runtime_recovers_from_a_panicking_task() {
        let mut runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();
        let bad = client.submit(TaskPayload::new("panic")).unwrap();
        let good = client.submit(TaskPayload::new("noop")).unwrap();

        assert_eq!(
            tick_until(&mut runtime, 2),
            2,
            "one panicking task stalled the runtime. Its failure must be reported \
             and its slot released, or the healthy task never gets dispatched."
        );

        let inner = runtime.inner.lock().unwrap();
        let results = inner.scheduler.get_results();
        assert!(
            !results[&bad].is_success(),
            "the panicking task should end as a Failure once retries run out"
        );
        assert!(
            results[&good].is_success(),
            "a healthy task must not be affected by a sibling that panicked"
        );
    }

    #[test]
    fn a_dead_worker_does_not_strand_its_tasks() {
        // The runtime sweeps for dead handles at the top of tick. Nothing kills a
        // thread outright today -- panics are caught -- so this test drives the
        // sweep through the scheduler directly and checks the requeue path is
        // wired up end to end.
        let mut runtime = LocalRuntime::new(2, 1);
        let mut client = runtime.client();
        let id = client.submit(TaskPayload::new("noop")).unwrap();

        // Dispatch it, then declare its worker dead before the result lands.
        {
            let mut inner = runtime.inner.lock().unwrap();
            let assignments = inner.scheduler.schedule();
            assert_eq!(assignments.len(), 1);
            let victim = assignments[0].worker_id;
            inner.scheduler.worker_offline(victim).unwrap();
        }

        assert_eq!(
            tick_until(&mut runtime, 1),
            1,
            "the task was in flight on a worker that went offline, so it must be \
             requeued and run on the survivor"
        );
        assert!(runtime.inner.lock().unwrap().scheduler.get_results()[&id].is_success());
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

impl Drop for LocalRuntime {
    /// Shut the pool down when the runtime handle goes away.
    ///
    /// Clients hold clones of the same `Arc`, so `RuntimeInner` can outlive
    /// this handle. The workers should not. `tick` only exists on
    /// `LocalRuntime`, so once this handle is gone nobody can drive the system
    /// and no task will ever run again. Keeping worker threads and worker
    /// processes alive past that point leaks them for nothing.
    fn drop(&mut self) {
        self.shutdown();
    }
}
