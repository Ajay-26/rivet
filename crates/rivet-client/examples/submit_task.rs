//! Demonstrates the intended end-to-end API.
//!
//! This example will panic on `client.submit(...)` until you complete
//! Milestone 1 (`LocalScheduler::submit`). That is intentional — the example
//! shows you the goal before you get there.
//!
//! Run with:
//!     cargo run --example submit_task

use rivet_client::LocalClient;
use rivet_core::TaskPayload;

fn main() {
    println!("=== Rivet: submit_task example ===");
    println!();

    let client = LocalClient::new();

    // ── Milestone 1 ──────────────────────────────────────────────────────────
    // Uncomment the block below once LocalScheduler::submit is implemented.
    //
    // let id = client
    //     .submit(TaskPayload::new("greet"))
    //     .expect("failed to submit task");
    // println!("Submitted task: {id}");
    //
    // ── Milestone 1 + worker ─────────────────────────────────────────────────
    // After implementing LocalWorker::execute, call client.tick() to run it:
    //
    // client.tick();
    //
    // match client.get_result(id).expect("failed to query result") {
    //     Some(result) => println!("Result: {:?}", result),
    //     None         => println!("Task still running — call tick() again."),
    // }

    println!("Nothing to run yet — see the TODO comments above.");
    println!("Complete Milestone 1 to bring this example to life.");

    // Suppress unused-variable warnings while the code above is commented out.
    let _ = client;
    let _ = TaskPayload::new("_");
}
