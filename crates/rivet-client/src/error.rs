use rivet_core::{RivetError, TaskId};
use std::fmt;

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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_names_the_thing_that_went_wrong() {
        let id = TaskId::new();
        assert!(ClientError::TaskNotFound(id)
            .to_string()
            .contains(&id.as_u64().to_string()));
        assert!(ClientError::ConnectionFailed(String::from("refused"))
            .to_string()
            .contains("refused"));
    }

    /// The wrapped error must not be swallowed, or a submit failure reads as
    /// "submit failed" with no cause.
    #[test]
    fn submit_failed_includes_the_underlying_cause() {
        let inner = RivetError::NoWorkersAvailable;
        let text = ClientError::SubmitFailed(inner).to_string();
        assert!(
            text.contains("no workers available"),
            "expected the RivetError message inside, got {text:?}"
        );
    }

    /// `?` in `LocalClient::submit` relies on this conversion existing.
    #[test]
    fn a_rivet_error_converts_into_a_client_error() {
        let converted: ClientError = RivetError::NoWorkersAvailable.into();
        assert!(matches!(
            converted,
            ClientError::SubmitFailed(RivetError::NoWorkersAvailable)
        ));
    }

    #[test]
    fn client_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientError>();
    }
}
