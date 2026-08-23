use rivet_client::{Client, LocalClient, LocalRuntime};
use rivet_core::{TaskId, TaskPayload};
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// These tests define the full client contract.
//
// From Milestone 4 the client is a handle: a `LocalRuntime` owns the scheduler
// and the worker pool, and `runtime.client()` hands out as many clients as you
// like onto that one runtime.
// ─────────────────────────────────────────────────────────────────────────────

/// Tick until `id` has a result, or give up. Bounded so a stranded task fails
/// the test instead of hanging the suite.
fn tick_until_done(runtime: &mut LocalRuntime, client: &LocalClient, id: TaskId) -> bool {
    for _ in 0..200 {
        runtime.tick();
        if client
            .get_result(id)
            .expect("get_result should not error")
            .is_some()
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Passes immediately — construction only.
#[test]
fn client_can_be_constructed() {
    let runtime = LocalRuntime::new(1, 1);
    let _client = runtime.client();
}

#[test]
fn submit_returns_a_task_id() {
    let runtime = LocalRuntime::new(1, 1);
    let mut client = runtime.client();
    let result = client.submit(TaskPayload::new("hello"));
    assert!(
        result.is_ok(),
        "submit should succeed for a valid payload: {:?}",
        result.err()
    );
}

#[test]
fn two_submissions_return_different_ids() {
    let runtime = LocalRuntime::new(1, 1);
    let mut client = runtime.client();
    let id1 = client.submit(TaskPayload::new("a")).unwrap();
    let id2 = client.submit(TaskPayload::new("b")).unwrap();
    assert_ne!(id1, id2, "each submission must receive a unique task ID");
}

#[test]
fn get_result_is_none_before_tick() {
    let runtime = LocalRuntime::new(1, 1);
    let mut client = runtime.client();
    let id = client.submit(TaskPayload::new("pending")).unwrap();

    // Submitted, but the runtime has not run yet.
    let result = client
        .get_result(id)
        .expect("get_result should not error for a known task");
    assert!(
        result.is_none(),
        "task should still be pending; got: {result:?}"
    );
}

#[test]
fn client_sees_result_after_tick() {
    let mut runtime = LocalRuntime::new(1, 1);
    let mut client = runtime.client();
    let id = client.submit(TaskPayload::new("noop")).unwrap();

    assert!(
        tick_until_done(&mut runtime, &client, id),
        "the task never completed"
    );
    assert!(client.get_result(id).unwrap().unwrap().is_success());
}

#[test]
fn two_tasks_complete() {
    let mut runtime = LocalRuntime::new(2, 1);
    let mut client = runtime.client();
    let id1 = client.submit(TaskPayload::new("noop")).unwrap();
    let id2 = client.submit(TaskPayload::new("noop")).unwrap();

    assert!(
        tick_until_done(&mut runtime, &client, id1),
        "task 1 stalled"
    );
    assert!(
        tick_until_done(&mut runtime, &client, id2),
        "task 2 stalled"
    );
    assert!(client.get_result(id1).unwrap().unwrap().is_success());
    assert!(client.get_result(id2).unwrap().unwrap().is_success());
}

/// Proves the handle split is real: two clients, one runtime, not two
/// independent systems.
#[test]
fn two_clients_share_one_runtime() {
    let mut runtime = LocalRuntime::new(2, 1);
    let mut alice = runtime.client();
    let mut bob = runtime.client();

    let a = alice.submit(TaskPayload::new("alice")).unwrap();
    let b = bob.submit(TaskPayload::new("bob")).unwrap();
    assert_ne!(a, b, "both clients feed one id counter and one scheduler");

    assert!(
        tick_until_done(&mut runtime, &alice, a),
        "alice's task stalled"
    );
    assert!(tick_until_done(&mut runtime, &bob, b), "bob's task stalled");

    assert!(alice.get_result(a).unwrap().is_some());
    assert!(bob.get_result(b).unwrap().is_some());
}

/// Results live in the runtime, not in the client that submitted the task, so
/// any client can read any result. That is a deliberate choice — a per-client
/// results view would need extra bookkeeping and buys nothing in-process.
#[test]
fn a_clients_task_is_visible_to_its_sibling() {
    let mut runtime = LocalRuntime::new(1, 1);
    let mut alice = runtime.client();
    let bob = runtime.client();

    let a = alice.submit(TaskPayload::new("shared")).unwrap();
    assert!(tick_until_done(&mut runtime, &alice, a), "the task stalled");

    assert!(
        bob.get_result(a).unwrap().is_some(),
        "results are runtime state, so bob should see alice's result"
    );
}
