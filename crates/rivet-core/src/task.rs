use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

/// A unique identifier for a task.
///
/// Backed by a monotonically increasing counter. Two `TaskId` values created
/// in the same process are guaranteed to be different.
///
/// TODO: In a distributed system, IDs must be unique across processes.
/// Consider switching to UUIDs (the `uuid` crate) or a distributed ID scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new() -> Self {
        TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task-{}", self.0)
    }
}

/// The data carried by a task: a name identifying what to run, and raw argument bytes.
///
/// TODO: Decide on a richer payload representation. Options:
///   a) Keep `Vec<u8>` and choose a serialization format (e.g. `bincode`, `serde_json`).
///   b) Use a trait object: `Box<dyn Fn() -> Vec<u8> + Send + 'static>`.
///   c) Make `Task` generic: `Task<T>` where `T: Serialize + Send`.
///
/// Each choice has different trade-offs for type safety, network transport, and ergonomics.
#[derive(Debug, Clone)]
pub struct TaskPayload {
    pub name: String,
    pub args: Vec<u8>,
}

impl TaskPayload {
    pub fn new(name: impl Into<String>) -> Self {
        TaskPayload {
            name: name.into(),
            args: Vec::new(),
        }
    }

    pub fn with_args(mut self, args: Vec<u8>) -> Self {
        self.args = args;
        self
    }
}

/// Where a task is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    /// Submitted but not yet given to a worker.
    Pending,
    /// Assigned to a worker, not yet running.
    Assigned,
    /// Currently executing on a worker.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed(String),
}

impl Default for TaskStatus {
    fn default() -> Self {
        TaskStatus::Pending
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "Pending"),
            TaskStatus::Assigned => write!(f, "Assigned"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Completed => write!(f, "Completed"),
            TaskStatus::Failed(msg) => write!(f, "Failed: {msg}"),
        }
    }
}

/// A task to be executed: its identity, what it should do, and its current state.
#[derive(Debug, Clone)]
pub struct Task {
    pub id: TaskId,
    pub payload: TaskPayload,
    pub status: TaskStatus,
}

impl Task {
    pub fn new(payload: TaskPayload) -> Self {
        Task {
            id: TaskId::new(),
            payload,
            status: TaskStatus::Pending,
        }
    }

    /// Returns `true` if this task has reached a terminal state (completed or failed).
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, TaskStatus::Completed | TaskStatus::Failed(_))
    }
}

/// The outcome of executing a task.
#[derive(Debug, Clone)]
pub enum TaskResult {
    Success { task_id: TaskId, output: Vec<u8> },
    Failure { task_id: TaskId, error: String },
}

impl TaskResult {
    /// Returns the ID of the task this result belongs to.
    pub fn task_id(&self) -> TaskId {
        match self {
            TaskResult::Success { task_id, .. } | TaskResult::Failure { task_id, .. } => *task_id,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, TaskResult::Success { .. })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_ids_are_unique() {
        let t1 = Task::new(TaskPayload::new("a"));
        let t2 = Task::new(TaskPayload::new("b"));
        assert_ne!(t1.id, t2.id, "each Task::new() must produce a distinct ID");
    }

    #[test]
    fn new_task_status_is_pending() {
        let task = Task::new(TaskPayload::new("test"));
        assert_eq!(task.status, TaskStatus::Pending);
    }

    #[test]
    fn task_is_not_terminal_when_pending() {
        let task = Task::new(TaskPayload::new("test"));
        assert!(!task.is_terminal());
    }

    #[test]
    fn task_is_terminal_when_completed() {
        let mut task = Task::new(TaskPayload::new("test"));
        task.status = TaskStatus::Completed;
        assert!(task.is_terminal());
    }

    #[test]
    fn task_is_terminal_when_failed() {
        let mut task = Task::new(TaskPayload::new("test"));
        task.status = TaskStatus::Failed("oh no".into());
        assert!(task.is_terminal());
    }

    #[test]
    fn task_result_success_reports_correct_id() {
        let id = TaskId::new();
        let result = TaskResult::Success { task_id: id, output: vec![] };
        assert_eq!(result.task_id(), id);
        assert!(result.is_success());
    }

    #[test]
    fn task_result_failure_is_not_success() {
        let id = TaskId::new();
        let result = TaskResult::Failure { task_id: id, error: "boom".into() };
        assert_eq!(result.task_id(), id);
        assert!(!result.is_success());
    }
}
