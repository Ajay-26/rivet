use crate::{Client, ClientError};
use rivet_core::{RivetError, Task, TaskId, TaskPayload, TaskResult, WorkerId, WorkerInfo};
use rivet_scheduler::{LocalScheduler, Scheduler};
use rivet_worker::{LocalWorker, Worker};
use std::thread::JoinHandle;

/// An in-process client that talks directly to a `LocalScheduler`.
///
/// No networking — scheduler and client live in the same process. This is the
/// right starting point: get the logic right locally before adding the
/// complexity of network transport.
///
/// TODO (Milestone 4): Replace `LocalScheduler` with a connection to a
/// scheduler running in a separate process or thread.
#[derive(Debug)]
pub struct LocalClient {
    scheduler: LocalScheduler,
    worker_count: usize,
}

impl LocalClient {
    pub fn new() -> Self {
        Self::with_workers(1)
    }

    pub fn with_workers(worker_count: usize) -> Self {
        let mut scheduler = LocalScheduler::new();
        for _ in 0..worker_count {
            scheduler
                .worker_registered(WorkerInfo::new(WorkerId::new()))
                .unwrap();
        }
        LocalClient {
            scheduler: scheduler,
            worker_count: worker_count,
        }
    }

    /// Drive the scheduler and collect any finished results.
    ///
    /// In a real system the scheduler runs on its own thread or process.
    /// Here you call this manually to advance the simulation.
    ///
    /// TODO (Milestone 2): Remove this and replace with a background thread
    /// that calls `scheduler.schedule()` on a timer or on every submission.
    pub fn tick(&mut self) {
        let assignments = self.scheduler.schedule();

        // TODO: For each assignment, dispatch the task to a LocalWorker,
        //       collect the result, and store it in self.results.
        let mut thread_results: Vec<JoinHandle<Result<TaskResult, RivetError>>> =
            Vec::with_capacity(self.worker_count);
        for elt in assignments.into_iter() {
            let handle = std::thread::spawn(move || {
                let worker: LocalWorker = LocalWorker::new();
                let result = worker.execute(elt.task);
                return result;
            });
            thread_results.push(handle);
        }

        for thread_result in thread_results {
            let result = thread_result.join().unwrap();
            match result {
                Ok(result) => {
                    let _task_id = match result {
                        TaskResult::Success { task_id, output: _ } => task_id,
                        TaskResult::Failure { task_id, error: _ } => task_id,
                    };
                    let _res = self.scheduler.worker_finished(result);
                    if _res.is_err() {
                        println!("Received error: {:?}", _res)
                    }
                }
                Err(error) => {
                    println!("Received error: {}", error);
                    return;
                }
            };
        }
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Client for LocalClient {
    fn submit(&mut self, payload: TaskPayload) -> Result<TaskId, ClientError> {
        let task = Task::new(payload);
        let id = self.scheduler.submit(task);
        Ok(id)
    }

    fn get_result(&self, id: TaskId) -> Result<Option<TaskResult>, ClientError> {
        Ok(self.scheduler.get_results().get(&id).cloned())
    }
}
