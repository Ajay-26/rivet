use std::fmt;
use crate::{TaskId, WorkerId};

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
