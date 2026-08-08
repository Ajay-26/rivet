use std::fmt;
use rivet_core::{RivetError, TaskId};

/// Errors that can occur when using the Rivet client.
///
/// TODO: Add a `SerializationError` variant once task payloads are encoded
/// for network transport.
#[derive(Debug)]
pub enum ClientError {
    /// Could not reach the scheduler (future: network errors).
    ConnectionFailed(String),
    /// The scheduler rejected the task submission.
    SubmitFailed(RivetError),
    /// Requested a result for a task ID that is not known.
    TaskNotFound(TaskId),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            ClientError::SubmitFailed(e) => write!(f, "submit failed: {e}"),
            ClientError::TaskNotFound(id) => write!(f, "task not found: {id}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<RivetError> for ClientError {
    fn from(e: RivetError) -> Self {
        ClientError::SubmitFailed(e)
    }
}
