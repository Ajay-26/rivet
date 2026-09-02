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
            Err(_) => Err(ClientError::SubmitFailed(RivetError::Other(String::from(
                "Submit failed",
            )))),
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalRuntime;

    /// The whole point of Milestone 4's split: the client is a handle, so it
    /// must be able to travel to another thread. This fails to compile if
    /// anything inside `RuntimeInner` is not `Send` — the `Box<dyn
    /// SchedulerPolicy>` needs its `+ Send` supertrait for this to hold.
    #[test]
    fn the_client_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LocalClient>();
    }

    #[test]
    fn a_client_really_can_be_used_from_another_thread() {
        let runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();
        let id = std::thread::spawn(move || client.submit(TaskPayload::new("remote")).unwrap())
            .join()
            .expect("the client should be usable off the main thread");

        assert!(runtime
            .client()
            .get_result(id)
            .expect("a submitted id is known")
            .is_none());
    }

    /// Clients hand out ids from one shared counter, not per-client counters.
    #[test]
    fn two_clients_do_not_hand_out_the_same_id() {
        let runtime = LocalRuntime::new(1, 1);
        let mut a = runtime.client();
        let mut b = runtime.client();
        assert_ne!(
            a.submit(TaskPayload::new("a")).unwrap(),
            b.submit(TaskPayload::new("b")).unwrap()
        );
    }

    /// `get_result` for an id that was never submitted is `Ok(None)`, the same
    /// answer as "still running". Documented here because it is a contract
    /// decision, not an accident — the client cannot distinguish the two.
    #[test]
    fn an_unknown_id_reads_as_pending() {
        let runtime = LocalRuntime::new(1, 1);
        let client = runtime.client();
        let orphan = TaskId::new();
        assert!(client.get_result(orphan).expect("no error").is_none());
    }

    /// `get_result` must not keep the lock held after it returns, or a caller
    /// that polls in a loop starves `tick`.
    #[test]
    fn get_result_releases_the_lock() {
        let mut runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();
        let id = client.submit(TaskPayload::new("noop")).unwrap();

        let _ = client.get_result(id);
        runtime.tick(); // deadlocks instead of failing if the guard leaked
        let _ = client.get_result(id);
    }
}
