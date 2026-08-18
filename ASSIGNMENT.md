# Rivet — Assignment Specification

> You are building a small distributed task-execution framework in Rust.
> The interfaces and architecture are provided. Your job is to make the tests
> pass, one milestone at a time.

---

## Background

Rivet is inspired by systems like [Ray](https://docs.ray.io/). A *client*
submits computational tasks. A *scheduler* decides which *worker* runs each
task. Workers execute tasks concurrently and report results back to the
scheduler, which makes them available to the client.

```
Client  →  Scheduler  →  Worker 1
                      →  Worker 2
                      →  Worker 3
```

The codebase is split into four crates in a Cargo workspace:

| Crate | Contains |
|---|---|
| `rivet-core` | All shared types (`Task`, `TaskId`, `TaskResult`, …) |
| `rivet-scheduler` | The `Scheduler` trait + `LocalScheduler` placeholder |
| `rivet-worker` | The `Worker` trait + `LocalWorker` placeholder |
| `rivet-client` | The `Client` trait, `LocalClient`, and the `rivet` CLI |

The project compiles from day one. Most of the interesting methods contain
`todo!()` — they will panic if called. Your job is to replace each `todo!()`
with a real implementation, milestone by milestone.

---

## Rules

1. **Do not change test function names or their `assert!` calls.** You may
   uncomment commented-out test code (several tests are intentionally commented
   out until the relevant milestone).
2. **Do not change the public API** (trait method signatures, public struct
   fields). You may add fields and helper methods freely.
3. **Prefer the standard library.** External crates are allowed from Milestone 6
   onward (see the README for a recommended list). If you add a crate earlier,
   explain why in a comment.
4. Run `cargo fmt` and `cargo clippy` before each milestone submission.
5. All tests in the current and all previous milestones must pass.

---

## Getting started

```bash
git clone <repo>
cd rivet
cargo build    # should succeed immediately
cargo test     # several tests will fail — that is expected
```

Read through the code before writing anything. Pay attention to:

- The `TODO` comments — they tell you *what* to do and *where*.
- The `#[test]` functions — they tell you *what a correct implementation looks like*.
- The design-question comments — they tell you where *you* get to make a
  decision.

---

## Milestone 1 — Local execution

**Objective:** tasks submitted via `LocalClient` are executed by a
`LocalWorker` in the same process.

### What to implement

#### `crates/rivet-scheduler/src/local.rs`

`LocalScheduler` must store submitted tasks and registered workers, then pair
them up when `schedule()` is called.

Add fields to `LocalScheduler`:

```rust
pending:  VecDeque<Task>
workers:  HashMap<WorkerId, WorkerInfo>
assigned: HashMap<TaskId, WorkerId>
```

Then implement:

| Method | Behaviour |
|---|---|
| `submit` | Push the task onto `pending`. Return `task.id`. |
| `schedule` | For each idle worker, pop one task from `pending`. Mark the worker Busy. Record the assignment. Return the list of `TaskAssignment`s. |
| `worker_registered` | Insert into `workers`. Return `Err(WorkerAlreadyRegistered)` on duplicate. |
| `worker_finished` | Look up the assignment. Mark the worker Idle. Store the result somewhere the client can retrieve it. Return `Err(TaskNotFound)` for unknown IDs. |

#### `crates/rivet-worker/src/local.rs`

Implement `LocalWorker::execute`:

1. Set `self.info.status = WorkerStatus::Busy`.
2. Return `TaskResult::Success { task_id: task.id, output: vec![] }`.
3. Set `self.info.status = WorkerStatus::Idle`.

For now, `output` can be empty — you are just wiring up the plumbing.

#### `crates/rivet-client/src/local.rs`

Implement `LocalClient::tick()`:

1. Call `self.scheduler.schedule()` to get assignments.
2. For each assignment, create a `LocalWorker`, call `worker.execute(task)`.
3. Call `self.scheduler.worker_finished(result)`.
4. Store the result in `self.results`.

You will need to store tasks somewhere accessible in `LocalClient`, or ask the
scheduler to return tasks alongside assignments.

### Tests that must pass after Milestone 1

```
rivet_scheduler::tests::scheduler_submit_preserves_task_id
rivet_scheduler::tests::scheduler_produces_no_assignments_without_workers
rivet_scheduler::tests::scheduler_registers_worker_without_error
rivet_scheduler::tests::scheduler_assigns_task_to_available_worker
rivet_scheduler::tests::scheduler_does_not_assign_same_task_twice
rivet_scheduler::tests::scheduler_worker_finished_marks_worker_idle
rivet_worker::tests::worker_starts_idle
rivet_worker::tests::worker_execute_returns_success_result
integration_test::submit_returns_a_task_id
integration_test::two_submissions_return_different_ids
integration_test::get_result_returns_none_for_pending_task
```

### Questions to answer (written, not in code)

1. Which data structure is most appropriate for `pending`? Compare `Vec`,
   `VecDeque`, and `BinaryHeap`. When would each be the right choice?
2. What does Rust's ownership system require you to do when moving a `Task`
   from the pending queue into a `TaskAssignment`? How is this different from
   Python or Java?
3. Why does `schedule` return `Vec<TaskAssignment>` rather than modifying
   worker state directly?

---

## Milestone 2 — Concurrent workers

**Objective:** multiple workers can execute tasks in parallel using OS threads.

### What to implement

- Spawn a thread inside `LocalWorker::execute`. The thread performs the work;
  the calling thread returns a `TaskResult` (either blocking with
  `JoinHandle::join()` for now, or using a channel for the non-blocking version).
- Add a `worker_count: usize` parameter to `LocalClient::new(count)` and
  register that many `LocalWorker`s with the scheduler on construction.
- Update `LocalClient::tick()` to dispatch multiple assignments per call.

### Tests that must pass

All Milestone 1 tests, plus:

```
rivet_worker::tests::worker_returns_to_idle_after_execution
```

(Uncomment and complete this test in `rivet-worker/src/lib.rs`.)

Also write a new integration test:

```rust
#[test]
fn two_tasks_complete_after_tick() {
    // Submit two tasks, call tick(), assert both have results.
}
```

### Questions to answer

1. What is `Send`? Why does the closure you pass to `std::thread::spawn` need
   to own its data rather than borrow it?
2. What happens if a worker thread panics? Does `JoinHandle::join()` propagate
   the panic? What should `LocalWorker::execute` do in that case?
3. What is the difference between data-race safety and deadlock safety in Rust?

---

## Milestone 3 — A second scheduling policy

**Objective:** make worker selection pluggable, and write a policy that provably
differs from picking the first available worker.

### What to implement

**1. Give workers a capacity.** In `crates/rivet-core/src/worker.rs`, add
`capacity` and `in_flight` counts to `WorkerInfo`, and narrow `WorkerStatus` to
liveness only (`Online` / `Offline`) — "busy" is now derivable from
`in_flight == capacity`, so storing it separately means two copies of one fact.
`is_available()` becomes "online and below capacity". Add guarded helpers to
increment and decrement `in_flight` rather than letting callers touch the field.

Without this step there is nothing to schedule *on*: a worker holding at most one
task has a load of 0 or 1, so "least loaded" and "first available" are the same
predicate and the two policies below are indistinguishable.

**2. Extract the policy.** Add `crates/rivet-scheduler/src/policy.rs` with a
`SchedulerPolicy` trait — one method that takes the worker map and the pending
queue and returns assignments. Move the selection logic out of
`LocalScheduler::schedule` and behind a `Box<dyn SchedulerPolicy>` field.

Keep the trait object-safe: no methods returning `Self`, no generic methods. A
constructor in the trait is the usual way to break this by accident.

**3. Implement two policies.** `FirstAvailablePolicy` takes any worker with
spare capacity. `LeastLoadedPolicy` takes the one with the lowest `in_flight` —
`min_by_key` is the whole algorithm. Recompute the minimum after each
assignment; assigning changes the thing you are selecting on.

Drive the loop off pending tasks, not workers. Iterating workers once caps you at
one assignment per worker regardless of capacity.

#### Implementation notes — `LeastLoadedPolicy`

- The shape is a `while` loop where each pass places exactly one task.
- To pick the worker: filter `workers.values_mut()` down to available ones and
  `min_by_key` on `in_flight`. That hands you an `Option<&mut WorkerInfo>` — the
  `None` case means nobody has room, so stop.
- Do that selection **inside** the loop. Hoisting it above holds one `&mut` for
  the whole loop, so you would keep handing tasks to the same worker.
- Order matters within a pass: confirm a worker is free *before* popping the
  task. Pop first and you drop the task on the floor when nobody can take it.
- `min_by_key` returns the first minimum it encounters, and `HashMap` order is
  arbitrary — so ties break nondeterministically. That is why
  `least_loaded_balances` asserts a spread of ≤ 1 rather than an exact
  placement.

### Tests

Unit tests in `policy.rs`. Build a `HashMap` of workers and a `VecDeque` of
tasks and call the policy directly — no scheduler or client needed.

| Test | Asserts |
|---|---|
| `respects_capacity` | one worker, capacity 2, three tasks → exactly 2 assignments, one task still pending |
| `fills_to_capacity` | one worker, capacity 3, three tasks → 3 assignments, all to that worker |
| `least_loaded_balances` | 3 workers capacity 2, 4 tasks → max load − min load ≤ 1. First-available can produce (2, 2, 0); least-loaded cannot. |

### Questions to answer

1. Write down an input where your two policies produce different assignments.
   If you cannot, one of them is not doing what its name claims.
2. `HashMap` iteration order is arbitrary and varies per run. Where does that
   leak into first-available's output, and does it matter?
3. Why does a constructor in a trait prevent `Box<dyn Trait>` from compiling?

### Not in scope

Workers still execute one task at a time in practice — `capacity` is enforced by
the scheduler's bookkeeping, not by anything in `rivet-worker`. Making a single
worker genuinely run several tasks at once, and making the client own a durable
worker pool, is Milestone 4.

---

## Milestone 4 — Worker communication via channels

**Objective:** workers and the scheduler communicate through channels, not
direct function calls.

### What to implement

Give each `LocalWorker` an inbox channel:

```rust
struct LocalWorker {
    info:   WorkerInfo,
    inbox:  mpsc::Sender<Task>,
    outbox: mpsc::Receiver<TaskResult>,
}
```

Each worker runs a background thread that:
1. Reads a `Task` from the inbox.
2. Executes it.
3. Sends a `TaskResult` to the outbox.

The scheduler sends tasks through `inbox`; the client polls `outbox` in `tick()`.

### Questions to answer

1. What is `Arc<Mutex<T>>` and when do you need it? Could you use channels
   instead of a mutex here?
2. What does the `Sync` marker trait mean? Which of your types need to be
   `Sync` to be shared across threads?
3. What is a *deadlock*? Write a scenario in which your channel-based design
   could deadlock.

---

## Milestone 5 — Fault tolerance

**Objective:** the system survives a worker crash and retries the failed task.

### What to implement

- Detect when a worker thread panics (use `JoinHandle::join` → `Err`, or
  `std::panic::catch_unwind` inside the thread).
- Mark the worker `Offline`.
- Requeue the failed task in `pending` (up to a configurable `max_retries`).
- Add a test that panics inside a task and verifies the result is
  `TaskResult::Failure` (or that a retry succeeds on the second attempt).

### Questions to answer

1. `std::panic::catch_unwind` is *not* a general error-handling mechanism —
   when should you use it, and when should you use `Result` instead?
2. What is the difference between *fail-stop* and *fail-noisy* failure models?
   Which does your implementation provide?

---

## Milestone 6 — Distributed execution

**Objective:** workers run as separate processes; the scheduler communicates
with them over TCP.

### What to implement

- Each worker binary binds a `TcpListener`, accepts one connection from the
  scheduler, and processes tasks sent over the socket.
- Choose a serialization format (`serde_json` is easiest; `bincode` is more
  compact).
- The scheduler connects to each registered worker's address and sends
  serialized `Task`s; workers reply with serialized `TaskResult`s.
- Update `WorkerInfo::address` from `Option<String>` to `Option<SocketAddr>`.

### Questions to answer

1. What can go wrong over a network that cannot happen with in-process
   channels? List at least three failure modes.
2. Is your wire protocol versioned? What happens if you deploy a new scheduler
   with old workers?

---

## Milestone 7 — Task graphs

**Objective:** tasks can declare dependencies on other tasks.

### What to implement

Add `depends_on: Vec<TaskId>` to `Task`. The scheduler must not dispatch a task
until all of its dependencies are in `TaskStatus::Completed`.

Implement a cycle-detection check in `submit` (or `schedule`): if a submitted
dependency graph contains a cycle, return an error immediately.

```
A ──┬──> B ──┐
    │        ├──> D
    └──> C ──┘
```

### Questions to answer

1. What algorithm did you use for cycle detection? What is its time complexity
   in terms of tasks and edges?
2. How should the scheduler handle a task whose dependency *failed*? Should it
   cancel the dependent tasks, retry the dependency, or propagate the failure?

---

## Grading criteria (suggested)

| Milestone | Weight | Passing condition |
|---|---|---|
| 1 | 25 % | All Milestone 1 tests green |
| 2 | 15 % | All Milestone 2 tests green |
| 3 | 10 % | Scheduling policy test passes |
| 4 | 15 % | Channel-based dispatch works end-to-end |
| 5 | 15 % | Fault-tolerance test passes |
| 6 | 10 % | Two processes exchange tasks over TCP |
| 7 | 10 % | Dependency graph test passes, cycle detection works |

Written answers to design questions are worth an additional mark per milestone
(assessed separately).

---

## Submitting

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

All three commands should exit with code 0 before you submit.

Commit your work with a message per milestone:

```
git commit -m "Milestone 1: local scheduler and worker"
```
