use crate::{TaskId, WorkerId};
use std::fmt;

/// Top-level error type shared across Rivet crates.
///
/// TODO: As the system grows, consider splitting this into per-crate error
/// types and composing them with `From` implementations, or use the `thiserror`
/// crate to reduce boilerplate.
#[derive(Debug)]
pub enum RivetError {
    TaskNotFound(TaskId),
    WorkerNotFound(WorkerId),
    WorkerAlreadyRegistered(WorkerId),
    NoWorkersAvailable,
    // TODO: Add IO / network error variants once workers communicate remotely.
    Other(String),
}

impl fmt::Display for RivetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RivetError::TaskNotFound(id) => write!(f, "task not found: {id}"),
            RivetError::WorkerNotFound(id) => write!(f, "worker not found: {id}"),
            RivetError::WorkerAlreadyRegistered(id) => {
                write!(f, "worker already registered: {id}")
            }
            RivetError::NoWorkersAvailable => write!(f, "no workers available"),
            RivetError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for RivetError {}

impl From<std::io::Error> for RivetError {
    fn from(err: std::io::Error) -> Self {
        RivetError::Other(err.to_string())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must render something a user can act on. A `Display` arm
    /// that forgets to include the id turns a specific error into a vague one.
    #[test]
    fn display_names_the_thing_that_went_wrong() {
        let task = TaskId::new();
        let worker = WorkerId::new();

        let cases = [
            (RivetError::TaskNotFound(task), task.as_u64().to_string()),
            (
                RivetError::WorkerNotFound(worker),
                worker.as_u64().to_string(),
            ),
            (
                RivetError::WorkerAlreadyRegistered(worker),
                worker.as_u64().to_string(),
            ),
        ];

        for (error, needle) in cases {
            let text = error.to_string();
            assert!(
                text.contains(&needle),
                "{error:?} rendered as {text:?}, which does not mention {needle}"
            );
            assert!(!text.is_empty());
        }
    }

    #[test]
    fn other_passes_the_message_through_unchanged() {
        let error = RivetError::Other(String::from("disk on fire"));
        assert_eq!(error.to_string(), "disk on fire");
    }

    #[test]
    fn no_workers_available_has_a_message() {
        assert_eq!(
            RivetError::NoWorkersAvailable.to_string(),
            "no workers available"
        );
    }

    /// `RivetError` crosses a thread boundary every time a worker thread reports
    /// a failure, and it will cross a process boundary at Milestone 6.
    #[test]
    fn rivet_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RivetError>();
    }

    #[test]
    fn rivet_error_is_a_std_error() {
        fn assert_error<T: std::error::Error>() {}
        assert_error::<RivetError>();
    }
}
