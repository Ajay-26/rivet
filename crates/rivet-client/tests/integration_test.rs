use rivet_client::{Client, LocalClient};
use rivet_core::TaskPayload;

// ─────────────────────────────────────────────────────────────────────────────
// These tests define the full client contract.
//
// Tests marked "passes immediately" check construction and types — no
// implementation needed.
//
// Tests marked "Milestone N" will panic (failing) until that milestone is done.
// ─────────────────────────────────────────────────────────────────────────────

/// Passes immediately — no scheduler code needed.
#[test]
fn client_can_be_constructed() {
    let _client = LocalClient::new();
}

/// Milestone 1 — requires LocalScheduler::submit.
#[test]
fn submit_returns_a_task_id() {
    let mut client = LocalClient::new();
    let result = client.submit(TaskPayload::new("hello"));
    assert!(
        result.is_ok(),
        "submit should succeed for a valid payload: {:?}",
        result.err()
    );
}

/// Milestone 1 — requires LocalScheduler::submit.
#[test]
fn two_submissions_return_different_ids() {
    let mut client = LocalClient::new();
    let id1 = client.submit(TaskPayload::new("a")).unwrap();
    let id2 = client.submit(TaskPayload::new("b")).unwrap();
    assert_ne!(id1, id2, "each submission must receive a unique task ID");
}

/// Milestone 1 — requires LocalScheduler::submit.
#[test]
fn get_result_returns_none_for_pending_task() {
    let mut client = LocalClient::new();
    let id = client.submit(TaskPayload::new("pending")).unwrap();

    // Task has been submitted but the scheduler hasn't run yet.
    let result = client
        .get_result(id)
        .expect("get_result should not error for a known task");
    assert!(
        result.is_none(),
        "task should still be pending; got: {:?}",
        result
    );
}

/// Milestone 1 + worker — requires both scheduler and LocalWorker::execute.
#[test]
fn tick_completes_a_submitted_task() {
    // TODO (Milestone 1/2): Uncomment and implement once execute works.
    //
    // let mut client = LocalClient::new();
    // client
    //     .worker_registered(rivet_worker::LocalWorker::new())
    //     .expect("registration should succeed");
    //
    // let id = client.submit(TaskPayload::new("noop")).unwrap();
    // client.tick();
    //
    // let result = client.get_result(id).unwrap();
    // assert!(result.is_some(), "task should be complete after tick");
    // assert!(result.unwrap().is_success());
}
