use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

/// A unique identifier for a worker.
///
/// TODO: Same distributed-uniqueness concern as `TaskId` — consider UUIDs
/// once workers run as separate processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(u64);

impl WorkerId {
    pub fn new() -> Self {
        WorkerId(NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn with_id(id: u64) -> Self {
        WorkerId(id)
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum WorkerStatus {
    #[default]
    Online,
    Offline,
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerStatus::Online => write!(f, "Online"),
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
    // TODO (Milestone 6, Step 5): make this `Option<SocketAddr>`.
    //
    // A `String` lets "localhsot:7001" travel all the way to a failed connect
    // at run time. A `SocketAddr` fails at the parse, next to the config that
    // was wrong. `with_address` should take `impl Into<SocketAddr>`.
    pub address: Option<SocketAddr>,
    pub capacity: usize,
    pub in_flight: usize,
}

impl WorkerInfo {
    pub fn new(id: WorkerId) -> Self {
        WorkerInfo {
            id,
            status: WorkerStatus::Online,
            address: None,
            capacity: 1,
            in_flight: 0,
        }
    }

    pub fn with_address(mut self, address: impl Into<SocketAddr>) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn is_available(&self) -> bool {
        (self.status == WorkerStatus::Online) && (self.in_flight < self.capacity)
    }

    pub fn add_inflight_task(&mut self) -> bool {
        if self.in_flight < self.capacity {
            self.in_flight += 1;
            true
        } else {
            false
        }
    }

    pub fn remove_inflight_task(&mut self) -> bool {
        if self.in_flight > 0 {
            self.in_flight -= 1;
            true
        } else {
            false
        }
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
    fn a_new_worker_is_online_and_available() {
        let info = WorkerInfo::new(WorkerId::new());
        assert_eq!(info.status, WorkerStatus::Online);
        assert!(info.is_available());
    }

    #[test]
    fn a_worker_at_capacity_is_not_available() {
        let mut info = WorkerInfo::new(WorkerId::new());
        info.status = WorkerStatus::Online;
        info.in_flight = info.capacity;
        assert!(
            !info.is_available(),
            "availability is capacity minus in_flight, not a Busy flag"
        );
    }

    #[test]
    fn default_capacity_is_one() {
        assert_eq!(
            WorkerInfo::new(WorkerId::new()).capacity,
            1,
            "a worker registered without with_capacity must accept exactly one task"
        );
    }

    #[test]
    fn with_capacity_raises_the_slot_count() {
        let info = WorkerInfo::new(WorkerId::new()).with_capacity(4);
        assert_eq!(info.capacity, 4);
        assert_eq!(info.in_flight, 0);
        assert!(info.is_available());
    }

    #[test]
    fn add_inflight_fills_up_to_capacity_then_refuses() {
        let mut info = WorkerInfo::new(WorkerId::new()).with_capacity(2);
        assert!(info.add_inflight_task());
        assert!(info.add_inflight_task());
        assert!(
            !info.add_inflight_task(),
            "the third add must be refused, or the policy can oversubscribe a worker"
        );
        assert_eq!(info.in_flight, 2, "a refused add must not change the count");
    }

    #[test]
    fn remove_inflight_refuses_to_go_below_zero() {
        let mut info = WorkerInfo::new(WorkerId::new()).with_capacity(1);
        assert!(!info.remove_inflight_task(), "nothing is in flight");
        assert_eq!(
            info.in_flight, 0,
            "usize would wrap, so this must be guarded"
        );
    }

    #[test]
    fn a_full_worker_becomes_available_again_after_a_completion() {
        let mut info = WorkerInfo::new(WorkerId::new()).with_capacity(1);
        info.add_inflight_task();
        assert!(!info.is_available());
        info.remove_inflight_task();
        assert!(
            info.is_available(),
            "this is the loop worker_finished closes; without it the pool wedges"
        );
    }

    #[test]
    fn an_offline_worker_stays_unavailable_even_with_free_slots() {
        let mut info = WorkerInfo::new(WorkerId::new()).with_capacity(4);
        info.status = WorkerStatus::Offline;
        assert!(
            !info.is_available(),
            "Milestone 5 relies on status alone taking a worker out of rotation"
        );
    }

    #[test]
    fn offline_worker_is_not_available() {
        let mut info = WorkerInfo::new(WorkerId::new());
        info.status = WorkerStatus::Offline;
        assert!(!info.is_available());
    }
}
