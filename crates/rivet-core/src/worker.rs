use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

/// A unique identifier for a worker.
///
/// TODO: Same distributed-uniqueness concern as `TaskId` — consider UUIDs
/// once workers run as separate processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(u64);

impl WorkerId {
    pub fn new() -> Self {
        WorkerId(NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker-{}", self.0)
    }
}

/// Whether a worker is able to accept new tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Offline,
}

impl Default for WorkerStatus {
    fn default() -> Self {
        WorkerStatus::Idle
    }
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerStatus::Idle => write!(f, "Idle"),
            WorkerStatus::Busy => write!(f, "Busy"),
            WorkerStatus::Offline => write!(f, "Offline"),
        }
    }
}

/// Everything the scheduler needs to know about a worker.
///
/// TODO: Once workers run as separate processes, `address` becomes mandatory
/// and should be a structured type (e.g. `std::net::SocketAddr`) rather than
/// an `Option<String>`.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub status: WorkerStatus,
    pub address: Option<String>,
}

impl WorkerInfo {
    pub fn new(id: WorkerId) -> Self {
        WorkerInfo {
            id,
            status: WorkerStatus::Idle,
            address: None,
        }
    }

    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn is_available(&self) -> bool {
        self.status == WorkerStatus::Idle
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_ids_are_unique() {
        let w1 = WorkerId::new();
        let w2 = WorkerId::new();
        assert_ne!(w1, w2, "each WorkerId::new() must produce a distinct ID");
    }

    #[test]
    fn new_worker_is_idle_and_available() {
        let info = WorkerInfo::new(WorkerId::new());
        assert_eq!(info.status, WorkerStatus::Idle);
        assert!(info.is_available());
    }

    #[test]
    fn busy_worker_is_not_available() {
        let mut info = WorkerInfo::new(WorkerId::new());
        info.status = WorkerStatus::Busy;
        assert!(!info.is_available());
    }

    #[test]
    fn offline_worker_is_not_available() {
        let mut info = WorkerInfo::new(WorkerId::new());
        info.status = WorkerStatus::Offline;
        assert!(!info.is_available());
    }
}
