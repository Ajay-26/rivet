pub mod error;
mod local;
mod runtime;

pub use error::ClientError;
pub use local::LocalClient;
pub use runtime::LocalRuntime;

use rivet_core::{TaskId, TaskPayload, TaskResult};

/// The public API a Rivet client exposes to application code.
///
/// # Example (future — works once Milestone 1 is complete)
///
/// let mut client = LocalClient::new();
/// let id = client.submit(TaskPayload::new("add"))?;
/// // ... later ...
/// if let Some(result) = client.get_result(id)? {
///     println!("Task finished: {:?}", result);
/// }
/// ```
pub trait Client {
    /// Submit a task for execution. Returns the task's ID.
    ///
    /// Same as `submit_with_dependencies` with an empty list, so an
    /// implementation only has to write the one method.
    fn submit(&mut self, payload: TaskPayload) -> Result<TaskId, ClientError> {
        self.submit_with_dependencies(payload, Vec::new())
    }

    /// Submit a task that must wait for other tasks to succeed first.
    ///
    /// Every id in `waits_for` must finish successfully before this task runs.
    /// An id that is not submitted yet is allowed — a client may build a graph
    /// in any order.
    ///
    /// Returns `Err` if the dependencies would close a cycle.
    fn submit_with_dependencies(
        &mut self,
        payload: TaskPayload,
        waits_for: Vec<TaskId>,
    ) -> Result<TaskId, ClientError>;

    /// Check whether a task has finished.
    ///
    /// Returns:
    ///   - `Ok(Some(result))` if the task is done.
    ///   - `Ok(None)` if the task is still pending or running.
    ///   - `Err(...)` if the task ID is unknown.
    ///
    /// TODO: Add a blocking variant — `wait_for_result(id, timeout)` — for
    /// callers that want to synchronously wait for a result.
    fn get_result(&self, id: TaskId) -> Result<Option<TaskResult>, ClientError>;
}
