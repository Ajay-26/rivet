use crate::runtime::RuntimeInner;
use crate::{Client, ClientError};
use rivet_core::{RivetError, Task, TaskId, TaskPayload, TaskResult};
use rivet_scheduler::Scheduler;
use std::sync::{Arc, Mutex};

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
    pub(crate) runtime: Arc<Mutex<RuntimeInner>>,
}

impl Client for LocalClient {
    fn submit(&mut self, payload: TaskPayload) -> Result<TaskId, ClientError> {
        let task = Task::new(payload);
        let runtime = self.runtime.lock();
        match runtime {
            Ok(mut runtime) => {
                let id = runtime.scheduler.submit(task);
                Ok(id)
            }
            Err(_) => {
                Err(ClientError::SubmitFailed(RivetError::Other(String::from(
                    "Submit failed",
                ))))
            }
        }
    }

    fn get_result(&self, id: TaskId) -> Result<Option<TaskResult>, ClientError> {
        let runtime = self.runtime.lock();
        match runtime {
            Ok(runtime) => Ok(runtime.scheduler.get_results().get(&id).cloned()),
            Err(_) => Err(ClientError::SubmitFailed(RivetError::Other(String::from(
                "Submit failed",
            )))),
        }
    }
}
