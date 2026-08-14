use crate::{Client, ClientError};
use rivet_core::{Task, TaskId, TaskPayload, TaskResult, WorkerId, WorkerInfo};
use rivet_scheduler::{LocalScheduler, Scheduler};
use rivet_worker::{LocalWorker, Worker};
use std::collections::HashMap;

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
    results: HashMap<TaskId, TaskResult>,
}

impl LocalClient {
    pub fn new() -> Self {
        let mut scheduler = LocalScheduler::new();
        scheduler.worker_registered(WorkerInfo::new(WorkerId::new())).unwrap();
        LocalClient {
            scheduler: scheduler,
            results: HashMap::new(),
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
        for elt in assignments.into_iter() {
            let mut worker: LocalWorker = LocalWorker::new();
            let result = worker.execute(elt.task);
            match result {
                Ok(res) => {
                    self.results.insert(elt.task_id, res);
                },
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
        Ok(self.results.get(&id).cloned())
    }
}
