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
    /// TODO (Milestone 7, Step 6):
    ///
    /// The same three lines as `submit` above, with one change: build the task
    /// with `Task::new(payload).with_dependencies(waits_for)` instead of
    /// `Task::new(payload)`.
    ///
    /// The scheduler takes the list off the task and files it in `waiting_on`
    /// and `blocks`, so nothing downstream of here needs to know about graphs.
    ///
    /// Once this works, delete `submit` above and let the trait default cover
    /// it.
    fn submit_with_dependencies(
        &mut self,
        _payload: TaskPayload,
        _waits_for: Vec<TaskId>,
    ) -> Result<TaskId, ClientError> {
        let task = Task::new(_payload).with_dependencies(_waits_for);
        let runtime = self.runtime.lock();
        match runtime {
            Ok(mut runtime) => {
                // `submit` can now fail — Milestone 7 rejects a dependency
                // cycle here. `From<RivetError> for ClientError` makes `?` work.
                let id = runtime.scheduler.submit(task)?;
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

// ── Milestone 7 tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod dependency_tests {
    use super::*;
    use crate::LocalRuntime;
    use std::time::{Duration, Instant};

    /// Tick until `id` has a result, or give up. Bounded on purpose: a cycle bug
    /// and a starvation bug both look like "never finishes", and a failure
    /// message is more use than a hung suite.
    fn tick_until(runtime: &mut LocalRuntime, client: &LocalClient, id: TaskId) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            runtime.tick();
            if client.get_result(id).unwrap().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    #[test]
    fn a_task_with_no_dependencies_still_works() {
        let mut runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();
        let id = client
            .submit_with_dependencies(TaskPayload::new("lonely"), Vec::new())
            .expect("an empty dependency list is always valid");

        assert!(tick_until(&mut runtime, &client, id), "the task never ran");
        assert!(client.get_result(id).unwrap().unwrap().is_success());
    }

    #[test]
    fn a_dependent_task_waits_for_its_dependency() {
        let mut runtime = LocalRuntime::new(2, 1);
        let mut client = runtime.client();

        let first = client.submit(TaskPayload::new("first")).unwrap();
        let second = client
            .submit_with_dependencies(TaskPayload::new("second"), vec![first])
            .unwrap();

        // One tick can only dispatch `first`, because `second` is blocked.
        runtime.tick();
        assert!(
            client.get_result(second).unwrap().is_none(),
            "second must not finish before first has even been dispatched"
        );

        assert!(
            tick_until(&mut runtime, &client, second),
            "second never ran"
        );
        assert!(
            client.get_result(first).unwrap().unwrap().is_success(),
            "first must have finished before second could start"
        );
    }

    #[test]
    fn the_runtime_completes_a_dependency_chain() {
        let mut runtime = LocalRuntime::new(2, 2);
        let mut client = runtime.client();

        let a = client.submit(TaskPayload::new("a")).unwrap();
        let b = client
            .submit_with_dependencies(TaskPayload::new("b"), vec![a])
            .unwrap();
        let c = client
            .submit_with_dependencies(TaskPayload::new("c"), vec![b])
            .unwrap();

        assert!(
            tick_until(&mut runtime, &client, c),
            "the chain never finished"
        );
        for id in [a, b, c] {
            assert!(
                client.get_result(id).unwrap().unwrap().is_success(),
                "every task in the chain should succeed"
            );
        }
    }

    // No cycle test here, and that is worth knowing rather than fixing.
    //
    // `Client` never lets you name a task id before you submit it —
    // `submit_with_dependencies` mints the id inside. So you can depend on an id
    // you invented, but you can never make a later task *have* that id. A cycle
    // is unreachable through this API.
    //
    // Cycles can only be built by code holding the scheduler directly, so
    // `a_cycle_is_rejected_at_submit` belongs in
    // `crates/rivet-scheduler/src/local.rs`.

    #[test]
    fn a_failed_dependency_gives_the_dependent_a_result() {
        let mut runtime = LocalRuntime::new(1, 1);
        let mut client = runtime.client();

        // "panic" always fails, and the default max_retries gives up eventually.
        let bad = client.submit(TaskPayload::new("panic")).unwrap();
        let waiting = client
            .submit_with_dependencies(TaskPayload::new("waiting"), vec![bad])
            .unwrap();

        assert!(
            tick_until(&mut runtime, &client, waiting),
            "the dependent never got a result. A dependency that fails for good \
             must fail what it blocks, or the client waits for ever."
        );
        assert!(
            !client.get_result(waiting).unwrap().unwrap().is_success(),
            "a task whose dependency failed cannot have succeeded"
        );
    }
}
