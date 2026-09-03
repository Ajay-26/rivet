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

## Milestone 4 — Long-lived workers, and a runtime to own them

**Objective:** stop calling workers and start *sending* to them. Each worker
becomes one or more threads blocked on an inbox channel, results come back over
a second channel — and a new `Runtime` type takes ownership of the scheduler and
the worker pool so the client can shrink to what a client should be.

Each step below has its own tests. Get each step green before starting the next
one; debugging channels and ownership at the same time is miserable, and most of
these tests need nothing from the steps that follow.

### Background

Two problems are being fixed at once, and they are related.

**Workers are objects you call.** `tick()` invokes `worker.execute(task)` and
waits. A real worker is a process that already exists, idle until work arrives —
something you *send to*. The Rust building block is `std::sync::mpsc`:
`mpsc::channel()` hands back a `(Sender<T>, Receiver<T>)` pair. Senders clone
freely; receivers do not, because only one place may take a value out.

**The client owns the whole system.** `LocalClient` currently holds the
scheduler, and after this milestone it would also hold the worker pool. That is
not a client — a client submits work and collects results. It should not know
that workers exist, let alone spawn them.

What is missing is a third role:

```
Client     submit / get_result. Holds a handle to the runtime. Nothing else.
Runtime    owns the scheduler and the worker pool. Spawns workers. Drives tick().
Scheduler  decides placement.  (unchanged this milestone)
Worker     executes a task.
```

Note the constraint that fixes where the runtime lives: `rivet-scheduler`
deliberately does not depend on `rivet-worker`, so the scheduler cannot own the
pool. The runtime must sit in a crate that depends on both — for now, a new
module inside `rivet-client`. Milestone 6 promotes it to its own process.

### The shape

```
LocalRuntime ─┬─ Arc<Mutex<RuntimeInner>> ──┬─ scheduler: LocalScheduler
              │                             ├─ workers: HashMap<WorkerId, WorkerHandle>
              │                             ├─ results_rx: Receiver<TaskResult>
              │                             └─ results: HashMap<TaskId, TaskResult>
              │
              └─ .client() ──▶ LocalClient ── same Arc, submit / get_result only

WorkerHandle { id, inbox: Sender<Task>, threads }
      │
      └── Arc<Mutex<Receiver<Task>>> ──┬── thread 1 ─┐
                                       ├── thread 2  ├─ execute ─▶ Sender<TaskResult>
                                       └── thread 3 ─┘
```

Two channels, different shapes, different reasons:

- **One task channel per worker.** The policy decides *which* worker gets a
  task; that only means something if each worker has its own queue.
- **One results channel for everybody.** The runtime does not care who finished
  what — the `TaskId` in the result is enough.

---

## Step 1 — `crates/rivet-worker/src/local.rs`: shrink `LocalWorker`

`LocalWorker` becomes a stateless executor. No channels, no status, no knowledge
that it is on a thread.

```rust
pub struct LocalWorker {
    id: WorkerId,
}
```

Change `Worker::execute` in `src/lib.rs` to take `&self` rather than
`&mut self`. This is load-bearing: several threads call `execute` on the same
worker concurrently, and `&mut self` would admit one at a time — concurrent code
that runs sequentially.

With status gone, `Worker::info() -> &WorkerInfo` is a lie: the worker no longer
maintains a `WorkerInfo`. Narrow it to `id(&self) -> WorkerId`.

Keep the `thread::sleep` in `execute`; you still need work that takes measurable
time.

**Expect to delete:** `worker_starts_idle` and
`worker_returns_to_idle_after_execution` — they assert on state that has moved.

### Tests — `crates/rivet-worker/src/lib.rs`

| Test | Asserts |
|---|---|
| `execute_returns_success_for_the_given_task` | keep the existing `worker_execute_returns_success_result`; the result's `task_id` matches the task |
| `worker_reports_its_id` | `id()` returns the `WorkerId` passed to `new` |
| `one_worker_executes_from_two_threads` | put a `LocalWorker` in an `Arc`, spawn two threads that each call `execute`, join both. **This does not compile if `execute` still takes `&mut self`** — that is the point of the test. |

---

## Step 2 — `crates/rivet-worker/src/handle.rs` (new file): `WorkerHandle`, `spawn`

### Deliverables

- [ ] New file `crates/rivet-worker/src/handle.rs`
- [ ] `mod handle;` **and** `pub use handle::{spawn, WorkerHandle};` in
      `crates/rivet-worker/src/lib.rs` — without the re-export, `spawn` is
      unreachable from `rivet-client` and Step 3 will not compile
- [ ] `struct WorkerHandle`
- [ ] `WorkerHandle::send` — the only way to reach the private `inbox`
- [ ] `fn spawn` — creates the channel, spawns the threads, returns the handle
- [ ] `impl Drop for WorkerHandle` — drop the sender, *then* join the threads
- [ ] The five tests at the end of this step

A new module, not an addition to `local.rs`. `LocalWorker` is *what a worker
does*; this is *how you reach one*.

Imports you will need in `handle.rs`:

```rust
use crate::{LocalWorker, Worker};
use rivet_core::{Task, TaskResult, WorkerId};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
```

The handle is the client-facing half of a running worker:

```rust
pub struct WorkerHandle {
    pub id: WorkerId,
    inbox: Option<mpsc::Sender<Task>>,
    threads: Vec<thread::JoinHandle<()>>,
}
```

The `Option` looks gratuitous now; the shutdown section below explains it.

`inbox` is private, so the handle needs a method for the runtime to dispatch
through — something like `send(&self, task: Task) -> Result<(), RivetError>`.
Decide what it should do when `inbox` is `None` (shutting down) or when the send
fails because every worker thread has died.

Then a free function in the same file. It creates the task channel, keeps the
receiver for the threads, and returns the sender inside the handle — so no
caller ever holds one half of a pair:

```rust
pub fn spawn(
    capacity: usize,
    results: mpsc::Sender<TaskResult>,
) -> WorkerHandle
```

Note there is no `id` parameter: `spawn` mints the `WorkerId` by constructing the
`LocalWorker`, and the caller reads it back off `handle.id`. That way ids come
from exactly one place, and the runtime cannot register a `WorkerInfo` under an
id that does not match a pool entry.

Inside:

1. `let (tx, rx) = mpsc::channel::<Task>();`
2. `let rx = Arc::new(Mutex::new(rx));` — all `capacity` threads share this.
3. `let worker = Arc::new(LocalWorker::new());` — likewise; take its id for the
   handle before you move it into the `Arc`.
4. Spawn `capacity` threads. **Clone the `Arc`s and the results `Sender` inside
   the loop**, before each `move` closure. A `move` closure takes ownership, so
   one clone declared above the loop is consumed by the first iteration and the
   second will not compile.
5. Return the handle with `id`, `Some(tx)`, and the join handles.

Each thread loops: take the lock, `recv()`, **release the lock**, execute, send
the result. `Err` from `recv()` means the channel closed — `break`.

> **Trap.** `while let Ok(task) = rx.lock().unwrap().recv() { ... }` compiles and
> is wrong. Temporaries in a `while let` scrutinee live for the whole body, so
> the guard is held while you execute and the other threads queue behind it.
> Capacity 3 silently becomes capacity 1. Bind it in a `let` statement instead —
> temporaries there drop at the semicolon:
> ```rust
> let received = {
>     let guard = rx.lock().unwrap();
>     guard.recv()
> }; // guard dropped here
> ```

Two things worth handling rather than ignoring: `execute` returns
`Result<TaskResult, RivetError>`, and anything you do not send onto the results
channel is a task that never completes. And `results.send(..)` itself returns a
`Result` — an `Err` means the runtime hung up, another reason to break.

### Shutdown

A worker thread exits when `recv()` returns `Err`, which happens only once
**every** `Sender` for its channel has been dropped. So the order is fixed:

1. Drop the inbox sender.
2. *Then* join the threads.

Join first and you wait forever on a thread politely blocked on a channel you
are still holding open.

That is what the `Option` is for. A `Drop` impl only gets `&mut self`, so you
cannot move the sender out of the struct — but `self.inbox.take()` leaves `None`
behind and hands you the value to drop. Same trick for the threads:
`self.threads.drain(..)` yields owned `JoinHandle`s you can `join()`.

Note what you get for free once `send` handles the `None` case: after shutdown,
`inbox` is `None`, so `send` returns `Err` instead of panicking.

Without this impl the threads are *detached* — dropping the handle still ends
them, because the inbox field drops with it, but nothing waits for them to
finish. `Drop` is what makes shutdown synchronous.

### Tests — `crates/rivet-worker/src/handle.rs`

These need no scheduler, no runtime, and no client. Build a results channel, call
`spawn`, send tasks, read results.

| Test | Asserts |
|---|---|
| `spawned_worker_executes_a_sent_task` | `spawn(id, 1, tx)`, send one task, `results_rx.recv()` returns a success whose `task_id` matches |
| `spawned_worker_handles_several_tasks_in_sequence` | capacity 1, send three tasks, receive three results; collect the ids and assert all three appear |
| `capacity_two_runs_two_tasks_concurrently` | capacity 2, send two tasks, time from send to second result with `Instant::now()`, assert well under 2 × the sleep. **This is the test that catches the `while let` lock trap** — with the guard held across `execute` it takes twice as long. |
| `dropping_the_handle_stops_the_threads` | `spawn`, then drop the handle (or call your shutdown), and assert it returns promptly rather than hanging |
| `send_after_shutdown_is_an_error` | after shutdown, `send` returns `Err` rather than panicking |

Tests inside `handle.rs` can reach private fields, so you can assert on
`threads.len() == capacity` directly if useful.

---

## Step 3 — `crates/rivet-client/src/runtime.rs` (new file): `LocalRuntime`

Register it in `crates/rivet-client/src/lib.rs`:

```rust
mod runtime;
pub use runtime::LocalRuntime;
```

The state lives in a private inner struct; the public type is a handle to it:

```rust
struct RuntimeInner {
    scheduler: LocalScheduler,
    workers: HashMap<WorkerId, WorkerHandle>,
    results_rx: mpsc::Receiver<TaskResult>,
    results: HashMap<TaskId, TaskResult>,
}

pub struct LocalRuntime {
    inner: Arc<Mutex<RuntimeInner>>,
}
```

Why the split: clients and the runtime need to reach the *same* state, possibly
from different threads. `Arc` gives shared ownership, `Mutex` gives safe
mutation, and wrapping a private inner struct means callers never see the
locking. It also lets `tick` take `&self` instead of `&mut self` — interior
mutability.

`LocalRuntime::new(worker_count, capacity)`:

1. Create the results channel once.
2. For each worker: mint a `WorkerId`, call `spawn(id, capacity, tx.clone())`,
   and register a `WorkerInfo::new(id).with_capacity(capacity)` on the
   scheduler — **the same id**. Mismatched ids mean the scheduler hands you
   assignments naming workers absent from your map.
3. Drop the runtime's own copy of the results `Sender` (see Step 5).

`LocalRuntime::client(&self) -> LocalClient` clones the `Arc` and wraps it.

`LocalRuntime::tick(&self)` has two halves:

- **Dispatch.** `schedule()`, then for each assignment look up
  `assignment.worker_id` and `send` the task to that handle.
- **Collect.** Drain `results_rx`, and for each result call
  `scheduler.worker_finished(result)` *before* storing it. That call is what
  returns capacity to the worker; without it `in_flight` only grows and the pool
  wedges after one round.

Use `try_recv()` for the drain — `Err(TryRecvError::Empty)` is your exit
condition and keeps `tick` from blocking. Add a helper that ticks in a loop
until every submitted task has a result, with a bounded attempt count so a bug
reports a failure instead of hanging the suite.

### Tests — `crates/rivet-client/src/runtime.rs`

Unit tests here can see `RuntimeInner`'s private fields, which is what makes the
first two possible.

| Test | Asserts |
|---|---|
| `pool_ids_match_registered_worker_ids` | `new(3, 1)`, then every key in `workers` is a worker the scheduler knows about. Catches the id-mismatch bug before it becomes a mystery. |
| `tick_on_an_idle_runtime_does_nothing` | `new(1, 1)` with nothing submitted; `tick()` returns promptly and `results` stays empty. Proves the drain is non-blocking. |
| `tick_dispatches_and_collects_one_task` | submit, tick until done, `results` contains the id |
| `capacity_is_returned_after_completion` | one worker capacity 1, two tasks. Tick until both complete. Fails if `worker_finished` is never called — the second task is stranded forever, so bound your loop. |
| `backlog_drains_over_several_ticks` | 2 workers × capacity 2, 10 tasks, tick until all 10 have results, assert termination |

---

## Step 4 — `crates/rivet-client/src/local.rs`: shrink `LocalClient`

The client becomes thin:

```rust
pub struct LocalClient {
    inner: Arc<Mutex<RuntimeInner>>,
}
```

`submit` builds a `Task` and forwards to the scheduler behind the lock;
`get_result` looks up the id in `results` and clones. That is all. No scheduler
field, no worker pool, no `tick`.

Usage becomes:

```rust
let runtime = LocalRuntime::new(4, 2);
let mut client = runtime.client();
let id = client.submit(TaskPayload::new("job"))?;
runtime.tick();
let result = client.get_result(id)?;
```

Notice what this buys: at Milestone 6 the client's `Arc<Mutex<..>>` becomes a
`TcpStream` and *only the client changes*. If the boundary is in the right place,
that swap is small.

Keep `Client::submit` as `&mut self` so the trait is untouched, even though
interior mutability no longer requires it.

### Tests — `crates/rivet-client/tests/integration_test.rs`

The existing tests all construct `LocalClient::new()` and will need rewriting
around `LocalRuntime::new(..).client()`. Their assertions should not change.

| Test | Asserts |
|---|---|
| `submit_returns_a_task_id` | (existing) still passes through the new path |
| `two_submissions_return_different_ids` | (existing) |
| `get_result_is_none_before_tick` | submit, do *not* tick, `get_result` is `None` |
| `client_sees_result_after_tick` | submit, tick until done, `get_result` is `Some` and successful |
| `two_clients_share_one_runtime` | two clients from one runtime, one task each, each sees its own result. Proves the handle split is real rather than two independent systems. |
| `a_clients_task_is_visible_to_its_sibling` | client A submits, client B calls `get_result` on A's id and finds it. Results live in the runtime, not the client — decide whether you *want* this, and assert whichever way you decide. |

---

## Step 5 — The runtime's side of shutdown

The handle's `Drop` (Step 2) takes care of the worker threads. One trap remains,
and it is the mirror image: **if the runtime keeps an unused clone of the results
`Sender`, the results channel never closes.** Hand every clone to a worker and
drop the original in the constructor.

### Tests

| Test | Asserts |
|---|---|
| `dropping_the_runtime_terminates_threads` | build `LocalRuntime::new(2, 2)`, drop it, and return promptly |
| `drop_after_work_terminates` | submit and complete a few tasks first, *then* drop. Different path: threads are mid-loop rather than freshly blocked. |
| `shutdown_is_idempotent` | explicit shutdown followed by drop does not panic or double-join |

A hanging test is worse than a failing one — it blocks the suite instead of
reporting. Do the drop on a spawned thread and assert it finishes within a
timeout, or use `recv_timeout` rather than `recv` when waiting on a channel to
close.

---

## Step 6 — `crates/rivet-scheduler/`: nothing

The `Scheduler` trait, `LocalScheduler`, and both policies are untouched.
Placement was already separated from delivery, so changing how tasks travel does
not disturb who decides where they go. If you find yourself editing this crate,
stop and work out why.

---

### End-to-end

One test that exercises the whole path once everything is green:

| Test | Asserts |
|---|---|
| `four_workers_run_four_tasks_concurrently` | `LocalRuntime::new(4, 1)`, four sleeping tasks, time the whole submit-and-drain cycle, assert well under 4 × the sleep. Measure *inside* the test — `cargo test` runs test functions in parallel, so suite duration proves nothing. |

### Questions to answer

1. `Receiver<T>` is `Send` but not `Sync`. What is the difference, and why does
   it force `Arc<Mutex<Receiver>>` rather than a plain `Arc<Receiver>`?
2. Every thread takes the same mutex to get a task. Why is that not a throughput
   bottleneck? What change would make it one?
3. Why must the sender be dropped before the threads are joined? Describe the
   deadlock precisely.
4. `tick()` takes `&self` while mutating everything behind it. Where did the
   `mut` go, and what is now enforced at runtime that used to be enforced at
   compile time?
5. What does the client know about workers now? Trace what would have to change
   in it if the runtime moved to another process.

---

## Milestone 5 — Fault tolerance

**Objective:** the system survives a worker crash and retries the failed task.

### Background

Two different failures hide under the word "crash", and they need different
handling.

- **A task panics.** The worker thread is fine; the *work* was bad. Catch the
  panic, report a `Failure`, and retry the task somewhere else.
- **A worker thread dies.** The panic escaped, or the thread exited. That
  worker's slots are gone. Mark it `Offline` so the policy stops choosing it.

Getting the first one wrong is worse than it looks: an uncaught panic inside
`spawn`'s thread loop kills one of the `capacity` threads permanently, and the
task that caused it is never reported, so `in_flight` never comes back down.
One bad task silently shrinks the pool.

---

## Step 1 — `crates/rivet-worker/src/local.rs`: give yourself a task that fails

`execute` currently sleeps and succeeds. You need a payload that panics, or
there is nothing to be tolerant of. Branch on `task.payload.name`: a name like
`"panic"` panics, everything else behaves as today. Two lines.

## Step 2 — `crates/rivet-worker/src/handle.rs`: catch it in the thread loop

Inside the `for` loop in `spawn`, the call is currently:

```rust
let result = worker.execute(task);
```

Wrap it:

```rust
let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.execute(task)));
```

Now you have three cases, not two: `Ok(Ok(r))`, `Ok(Err(e))`, and
`Err(Box<dyn Any + Send>)` for the panic. All three must send something on
`result_sender` — a `TaskResult::Failure` for the last two. The thread must
`continue`, not `break`.

`AssertUnwindSafe` is needed because `&LocalWorker` crosses the unwind
boundary and is not `UnwindSafe`. You are asserting that a panic cannot leave
the worker in a broken state, which is true here because `LocalWorker` holds
only an id.

Also add a way for the runtime to notice a dead thread:

```rust
pub fn is_alive(&self) -> bool   // threads.iter().any(|t| !t.is_finished())
```

## Step 3 — `crates/rivet-scheduler/`: retry bookkeeping

`worker_finished` receives a `TaskResult`, not a `Task`. To requeue you need
the task back — and `schedule` already removed it from `pending` when it
dispatched it, so `pending` is not where you look. Between dispatch and result
the only copy lives inside the worker thread. The scheduler has to keep one.

You already have a type that holds exactly what is needed:

```rust
pub struct TaskAssignment { task_id: TaskId, worker_id: WorkerId, task: Task }
```

So widen the map to hold the whole assignment:

```rust
assigned: HashMap<TaskId, TaskAssignment>,
```

In `schedule`, the insert becomes `self.assigned.insert(a.task_id, a.clone())`
instead of `.insert(a.task_id, a.worker_id)`. `Task` already derives `Clone`.

A tuple — `HashMap<TaskId, (WorkerId, Task)>` — holds the same data, but you
pay for it at every call site in `.0` and `.1`.

The design a production scheduler uses is different again: one
`tasks: HashMap<TaskId, Task>` as the single owner, with `pending: VecDeque<TaskId>`
holding only ids, so requeueing clones nothing. It is the better answer and it
is out of scope here, because `SchedulerPolicy::schedule` takes
`&mut VecDeque<Task>` and both policies would change with it. Milestone 7 needs
lookup-by-id anyway; revisit it there.

Then in `local.rs` add:

```rust
attempts: HashMap<TaskId, u32>,
max_retries: u32,
```

and a `LocalScheduler::with_max_retries(n)` constructor next to `with_policy`.

`worker_finished` grows a branch. On `TaskResult::Success`, behave as today. On
`TaskResult::Failure`, bump `attempts`; if it is below `max_retries`, push the
task back onto `pending` and *still* call `remove_inflight_task` so the slot is
released; otherwise store the failure as the final result.

Add one method to the `Scheduler` trait in `crates/rivet-scheduler/src/lib.rs`:

```rust
fn worker_offline(&mut self, id: WorkerId) -> Result<(), RivetError>;
```

It sets `WorkerStatus::Offline` and requeues every task still assigned to that
worker. `WorkerInfo::is_available` already checks the status, so the policy
needs no change at all — that is the payoff for narrowing `WorkerStatus` in
Milestone 3.

## Step 4 — `crates/rivet-client/src/runtime.rs`: notice dead workers

In `tick`, before dispatch, sweep the pool: for any handle where `is_alive()`
is false, call `scheduler.worker_offline(id)`. Do it before `schedule()` so the
same tick does not hand work to a corpse.

### Tests

| Test | File | Asserts |
|---|---|---|
| `a_panicking_task_returns_a_failure` | `rivet-worker/src/handle.rs` | send a `"panic"` task, a `TaskResult::Failure` arrives |
| `a_panic_does_not_kill_the_worker_thread` | `rivet-worker/src/handle.rs` | capacity 1: send `"panic"`, then a normal task; the second still completes |
| `a_failed_task_is_retried` | `rivet-scheduler/src/local.rs` | `max_retries` 1, feed `worker_finished` a `Failure`; the task is back in `pending` |
| `retries_stop_at_the_limit` | `rivet-scheduler/src/local.rs` | feed `max_retries + 1` failures; the last is stored as the result and `pending` is empty |
| `an_offline_worker_gets_no_assignments` | `rivet-scheduler/src/local.rs` | two workers, mark one offline, submit two tasks; both go to the survivor |
| `the_runtime_recovers_from_a_panicking_task` | `rivet-client/src/runtime.rs` | submit a `"panic"` task and a normal one; tick until done; both have results and the normal one succeeded |

### Questions to answer

1. `std::panic::catch_unwind` is *not* a general error-handling mechanism —
   when should you use it, and when should you use `Result` instead?
2. What is the difference between *fail-stop* and *fail-noisy* failure models?
   Which does your implementation provide?
3. A task that panics deterministically will panic on every retry. What stops
   `max_retries` from turning one bad task into N wasted slots?

---

## Milestone 6 — Distributed execution

**Objective:** workers run as separate OS processes; the runtime talks to them
over TCP. The scheduler, the policies, and the client do not change.

### Background

This is the milestone Milestone 4 was built for. The runtime reaches a worker
through exactly three things — `send`, `is_alive`, and results arriving on an
`mpsc::Receiver`. Nothing else. So a worker that lives on the far end of a
socket can be made to look identical, and `tick()` never learns the difference.

Three things are genuinely hard here, and none of them existed in-process.

**TCP has no messages.** A channel moves a `Task`. A socket moves bytes. One
`write` of 400 bytes can arrive as `read`s of 130 and 270, or two writes can
arrive as one read. You must impose your own message boundaries. This is called
*framing*, and forgetting it is the classic first networking bug — it works on
localhost with small payloads and breaks the moment a payload grows.

**Rust cannot send code.** A `Task` is a name and a `Vec<u8>`, not a closure,
which is why `TaskPayload` was designed that way in Milestone 1. Both processes
must already contain the function the name refers to.

**Ids are process-local.** `TaskId` and `WorkerId` are `AtomicU64` counters
(`rivet-core/src/task.rs`). Two worker processes both mint `WorkerId(1)`. Task
ids are safe because only the runtime creates them, but worker ids are not —
fix this by having the runtime assign them at handshake, not the worker.

---

## Step 0 — dependencies

The first crates in the project. In `crates/rivet-core/Cargo.toml`:

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Add the same two to `rivet-worker` and `rivet-client`.

Use **newline-delimited JSON**: one message per line, `\n` as the delimiter.
This is not laziness — it makes framing a solved problem, because
`BufReader::lines()` does it for you. `bincode` is more compact and needs a
length prefix you write yourself; do that later if you want the exercise.

## Step 1 — `crates/rivet-core`: make the types serializable

### What serde actually is

Serde is not a file format. It is **two traits**:

```rust
trait Serialize   { /* turn me into a stream of fields */ }
trait Deserialize { /* build me from a stream of fields */ }
```

A type that implements `Serialize` can describe itself as "a struct with three
fields, the first is a u64, ...". It does *not* decide what bytes come out.
That is the format crate's job. `serde_json` turns those descriptions into JSON;
`bincode` turns the very same descriptions into packed binary. This is why you
add two crates, not one: `serde` is the vocabulary, `serde_json` is the language.

Writing those impls by hand is tedious, so serde ships a macro:

```rust
#[derive(Serialize, Deserialize)]
struct Task { /* ... */ }
```

That macro lives behind serde's `derive` feature — which is why the root
`Cargo.toml` says `features = ["derive"]`. Without it the derive is not in
scope and you get "cannot find derive macro `Serialize`".

### The derives you need

In `crates/rivet-core/src/task.rs`, add `Serialize, Deserialize` to the derive
lists on `TaskId`, `TaskPayload`, `TaskStatus`, `Task`, and `TaskResult`. In
`src/worker.rs`, the same on `WorkerId`.

It has to be all of them. Derives are not inherited — the generated code for
`Task` calls `self.payload.serialize(..)`, so `TaskPayload` must implement it
too, and `TaskPayload` contains a `String` and a `Vec<u8>`, which serde already
covers. If you derive on `Task` alone you get:

```
error[E0277]: the trait bound `TaskPayload: Serialize` is not satisfied
```

Read that error as "you missed one, and here is which". Work up from the
innermost type.

At the top of each file you will need:

```rust
use serde::{Deserialize, Serialize};
```

### What comes out the other end

Worth knowing before you debug it at 1am. Given `Task { id: TaskId(7), payload:
TaskPayload { name: "add", args: vec![1, 2] }, status: TaskStatus::Pending }`:

```json
{"id":7,"payload":{"name":"add","args":[1,2]},"status":"Pending"}
```

Three things to notice.

- `TaskId(7)` became a bare `7`. A tuple struct with exactly one field is
  transparent — serde assumes the wrapper is a Rust-side nicety, not data.
- `TaskStatus::Pending` became the string `"Pending"`. An enum variant with no
  data serializes as its name.
- A variant *with* data becomes an object keyed by the variant name.
  `TaskStatus::Failed("boom")` is `{"Failed":"boom"}`, and
  `TaskResult::Success { task_id, output }` is
  `{"Success":{"task_id":7,"output":[]}}`. Serde calls this "externally
  tagged", and it is the default.

Also notice `args: [1,2]`. A `Vec<u8>` in JSON is an array of numbers, one
decimal per byte, so a 1 KB payload becomes roughly 4 KB of text. Correct, just
fat. `serde_bytes` or a base64 field fixes it if you care; do not bother yet.

### The wire protocol

New file `crates/rivet-core/src/wire.rs`, added to `src/lib.rs` with
`pub mod wire;`. Two enums, both deriving the same pair:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerToRuntime {
    Hello { capacity: usize, version: u32 },
    Finished(TaskResult),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RuntimeToWorker {
    Welcome { worker_id: WorkerId },
    Run(Task),
}
```

**Group them by direction, and name them for it.** The obvious split is
`Request` and `Response`, and here it is wrong: the worker sends the first
message *and* the results, while the runtime replies *and* issues the work. Both
sides send both kinds of thing, so knowing a message is a "request" tells you
nothing about who wrote it — you end up memorising all four. Named by direction,
the type answers the question.

One enum per direction also means adding a message later cannot silently break
the other side — you get a non-exhaustive `match` error instead.

On the wire those look like:

```json
{"Hello":{"capacity":2,"version":1}}
{"Run":{"id":9,"payload":{"name":"x","args":[255]},"status":"Pending"}}
```

`Hello` carries the worker's capacity, so the runtime does not have to guess how
many slots to register. `Welcome` carries the id the runtime assigned, which is
the fix for the collision in the Background. `version` is Question 2: compare it
on receipt and refuse a mismatch loudly.

### Framing, and why newlines

TCP gives you a stream of bytes with no message boundaries. If you write two
messages and call `read` once, you may get the first, both, or one and a half.
You need a rule for where one message ends.

The rule here is: **one JSON message per line**. This works because JSON never
contains a raw newline — a newline inside a string is escaped as `\n`, two
characters — so an unescaped `\n` is unambiguously a message boundary. And
`BufRead::read_line` already stops at one, so the whole problem is handled by
the standard library.

### The two helpers

```rust
pub fn write_message<W: Write, T: Serialize>(w: &mut W, msg: &T) -> io::Result<()>
pub fn read_message<R: BufRead, T: DeserializeOwned>(r: &mut R) -> io::Result<Option<T>>
```

You will need these imports: `serde::Serialize`, `serde::de::DeserializeOwned`,
and `std::io::{self, BufRead, Write}`.

The tools are already in the two crates you added. For writing, look at
`serde_json::to_writer` — note that it does **not** add a trailing newline, so
the delimiter is yours to append. For reading, `BufRead::read_line` fills a
`String` and returns how many bytes it read; `serde_json::from_str` turns that
into your type. Neither function is more than about five lines.

Five things to work out rather than guess at.

**Why generic over `W` and `T`.** One pair of functions has to serve both wire
enums, over a `TcpStream`, a `Vec<u8>`, or a file. The tests below use
an in-memory buffer precisely because the functions do not care what they are
writing to.

**Why `DeserializeOwned` and not `Deserialize`.** This is the one that confuses
everybody. `Deserialize<'de>` allows a type to *borrow* from the input buffer —
a `&'de str` field can point straight into the bytes you parsed, with no
copying. Fast, but the value cannot outlive the buffer. Ask yourself where the
buffer inside `read_message` lives and when it dies, and the reason the bound
has to be the owned one will be obvious. Our types use `String` and `Vec`, so
they satisfy it without any work from you.

**Flushing.** Buffered writers hold bytes back. Work out what happens if the
last write of a request never reaches the socket while both sides are waiting
to read. This failure has no error message — the program simply stops — so
decide now where the flush belongs.

**Signalling that the peer hung up.** `read_line` has a specific return value
for a clean end of stream, distinct from an IO error. That case is normal
shutdown, not a failure, and the caller needs to tell them apart. That is why
the return type is `Option` nested inside `Result`; make sure each of the three
outcomes maps to the right one.

**Error conversion.** `?` on the `serde_json` call inside a function returning
`io::Result` compiles, because `serde_json::Error` has a `From` impl into
`io::Error`. Malformed JSON therefore surfaces as an `Err`, which is correct: it
means the peer is broken or speaking a different protocol version.

### Why this lives in `rivet-core`

Both ends need these functions, and `rivet-worker` and `rivet-client` do not
depend on each other — look at the dependency graph in the README. `rivet-core`
is the only crate both can see.

### Tests for this step

Write these before Step 2. A framing bug found here takes a minute; the same bug
found through two processes and a socket takes an afternoon.

| Test | Asserts |
|---|---|
| `wire_round_trips_every_message` | write then read every variant of both enums into a `Vec<u8>`, get the original back |
| `read_message_reassembles_a_split_write` | feed the bytes in two chunks; still one whole message out |
| `read_message_returns_none_at_eof` | an empty reader gives `Ok(None)`, not `Err` |
| `two_messages_in_one_buffer_read_back_as_two` | write A then B, read twice, get A then B — catches a reader that swallows the rest of the buffer |
| `a_task_survives_the_round_trip_intact` | id, payload name, and args all match after the trip |

For a `Vec<u8>` buffer, write with `&mut buf`, then read with
`&mut buf.as_slice()` — `&[u8]` implements `BufRead`, so no socket is needed.

## Step 2 — `crates/rivet-worker/src/bin/rivet-worker.rs` (new file): the worker binary

A second binary in the workspace. It takes a bind address on the command line,
binds a `TcpListener`, and accepts one connection.

The useful realisation is that you already have the inside of this. `spawn`
gives you a `capacity`-sized thread pool with panic handling and a results
channel. So the binary is a pump:

1. `let handle = spawn(capacity, results_tx)` — reuse Milestones 4 and 5 whole.
2. `stream.try_clone()` to get a second owned handle to the same socket, so one
   thread can read while another writes.
3. Read `RuntimeToWorker`s in a loop; each `Run(task)` becomes `handle.send(task)`.
4. A second thread drains `results_rx` and writes `WorkerToRuntime::Finished` out.

`catch_unwind` is already inside `spawn`'s loop, so a panicking task still
cannot take this process down. That is Milestone 5 paying for itself.

## Step 3 — `crates/rivet-worker/src/remote.rs` (new file): a handle over a socket

The runtime's map is `HashMap<WorkerId, WorkerHandle>` today, one concrete type.
Make it hold either kind, with the same trait-object pattern as
`SchedulerPolicy`:

```rust
pub trait WorkerTransport: std::fmt::Debug + Send {
    fn send(&self, task: Task) -> Result<(), RivetError>;
    fn is_alive(&self) -> bool;
}
```

Implement it for `WorkerHandle` — both methods already exist, so the impl block
is two forwarding lines — and for the new `RemoteWorkerHandle`:

```rust
pub struct RemoteWorkerHandle {
    id: WorkerId,
    stream: Arc<Mutex<TcpStream>>,      // write half
    alive: Arc<AtomicBool>,             // reader thread clears this
    reader: Option<JoinHandle<()>>,
}
```

`connect(addr, results_tx)` does the handshake, then spawns **one reader
thread** whose whole job is: `read_message::<WorkerToRuntime>` in a loop, and forward
each `Finished(result)` into `results_tx`. That is the trick that makes this
transparent — the runtime keeps draining the same `mpsc::Receiver` it always
did, and `tick()` needs no change at all.

On EOF or error the reader sets `alive` to `false` and exits. So `is_alive()`
finally means something: kill the worker process and the next `tick` sweeps it
offline and requeues its tasks. Milestone 5's Step 4 sweep was untestable
in-process because a caught panic never kills a thread. Now it is testable.

`send` locks the stream and calls `write_message`. Writes are small, so a mutex
is fine; a dedicated writer thread fed by a channel is the alternative if you
want `send` to never block.

## Step 4 — `crates/rivet-client/src/runtime.rs`: connect instead of spawn

Change the field type:

```rust
workers: HashMap<WorkerId, Box<dyn WorkerTransport>>,
```

Add a constructor beside `new`:

```rust
pub fn with_remote_workers(addrs: &[SocketAddr]) -> io::Result<Self>
```

For each address: connect, handshake, mint the `WorkerId` **here**, and register
`WorkerInfo::new(id).with_capacity(capacity_from_hello).with_address(addr)`. The
existing `new(worker_count, capacity)` keeps working and boxes `WorkerHandle`s
instead.

`tick()` does not change. If it does, the abstraction is in the wrong place —
go back and look at why.

## Step 5 — `crates/rivet-core/src/worker.rs`: type the address

`address: Option<String>` becomes `Option<SocketAddr>`, and `with_address` takes
`impl Into<SocketAddr>`. A `String` lets `"localhsot:70001"` reach runtime;
`SocketAddr` fails at the parse, next to the config that was wrong.

### Tests

| Test | File | Asserts |
|---|---|---|
| `wire_round_trips_every_message` | `rivet-core/src/wire.rs` | every variant of both enums survives write-then-read |
| `read_message_reassembles_a_split_write` | `rivet-core/src/wire.rs` | write one message in two chunks with a pause between; the reader still returns one whole message. **This is the framing test** — it fails if you used `read` instead of `lines`/`read_line` |
| `read_message_returns_none_at_eof` | `rivet-core/src/wire.rs` | a closed peer is `Ok(None)`, not `Err` |
| `remote_handle_executes_a_task` | `rivet-worker/src/remote.rs` | stand up a `TcpListener` on `127.0.0.1:0` in the test, connect, send a task, assert a result arrives on `results_rx` |
| `remote_handle_notices_a_closed_socket` | `rivet-worker/src/remote.rs` | drop the far end; `is_alive()` becomes false |
| `a_killed_worker_process_requeues_its_tasks` | `rivet-client/tests/` | spawn two real worker processes, submit, kill one mid-task, tick until done, assert every task still has a result |
| `two_worker_processes_get_distinct_ids` | `rivet-client/tests/` | the collision from the Background section; fails if the worker mints its own id |

Use `127.0.0.1:0` and read back the bound port with `TcpListener::local_addr()`
— never hard-code a port, or your tests collide with each other and with
whatever else is on the machine.

For the process-level tests, you need the path to the built worker binary.
`env!("CARGO_BIN_EXE_rivet-worker")` is the usual answer, but cargo only defines
it for tests inside the crate that *declares* the binary — and these tests live
in `rivet-client`. Derive it from `std::env::current_exe()` instead: the test
binary sits in `target/<profile>/deps/`, so the worker is one directory up.
Assert the file exists and say so in the message, because
`cargo test -p rivet-client` on its own will not have built it.

Kill child processes in `Drop`, not at the end of the test. `Drop` runs while a
panic unwinds; code after a failed assertion does not. Leaked workers hold their
ports and make the next run fail for the wrong reason.

### Questions to answer

1. What can go wrong over a network that cannot happen with in-process
   channels? List at least three failure modes.
2. Is your wire protocol versioned? What happens if you deploy a new scheduler
   with old workers?
3. A worker acknowledges a task, then dies before reporting. You requeue it and
   it runs elsewhere. Now suppose it had already finished and the *reply* was
   lost. What did the client observe, and what would you need for
   exactly-once instead of at-least-once?

---

## Milestone 7 — Task graphs

**Objective:** a task can declare that it must not run until other tasks have
succeeded.

```
A ──┬──> B ──┐
    │        ├──> D
    └──> C ──┘
```

### Background

Every milestone so far treated `pending` as "runnable". Now it is only
"submitted", and a second question sits in front of worker selection: *is this
task allowed to run yet?*

Keep those two questions apart. **Eligibility** is the scheduler's job —
it owns the results. **Placement** is the policy's job. If you put a
`depends_on` check inside `FirstAvailablePolicy`, you have to write it again in
`LeastLoadedPolicy`, and every future policy inherits the bug. This is the same
boundary as the runtime/scheduler split in Milestone 4, one level down.

Three cases are easy to miss, and each has a test below:

- A dependency **fails permanently**. The dependent can never become eligible.
  If you only ever gate on success, it sits in `pending` forever and the client
  blocks on a result that will never come.
- A dependency is **retried**. It failed once but has attempts left, so the
  dependent must stay blocked without being cancelled.
- The graph contains a **cycle**. Nothing is ever eligible and `tick` spins
  quietly. A silent hang is the worst failure mode in the project; make it a
  loud error at submit time instead.

---

## Step 1 — `crates/rivet-core/src/task.rs`: the edge list

```rust
pub struct Task {
    pub id: TaskId,
    pub payload: TaskPayload,
    pub status: TaskStatus,
    pub depends_on: Vec<TaskId>,
}
```

`Task::new` sets it empty, so every existing call site keeps compiling. Add a
builder next to the ones in `WorkerInfo`:

```rust
pub fn with_dependencies(mut self, deps: Vec<TaskId>) -> Self
```

Edges point from dependent to dependency, which is the direction you need when
asking "can I run?". Building the reverse index — dependency to dependents — is
Step 4's problem, and it is only an optimisation.

## Step 2 — `crates/rivet-scheduler/src/lib.rs`: `submit` has to be able to fail

```rust
fn submit(&mut self, task: Task) -> Result<TaskId, RivetError>;
```

It returns a bare `TaskId` today, so there is no way to report a cycle. Change
the trait. `Client::submit` in `crates/rivet-client/src/lib.rs` already returns
`Result<TaskId, ClientError>`, so this plumbs straight through — the only real
work is the `?` in `LocalClient::submit` and updating the existing tests.

Add to `crates/rivet-core/src/error.rs`:

```rust
DependencyCycle(TaskId),
UnknownDependency { task: TaskId, dependency: TaskId },
```

and their `Display` arms. `UnknownDependency` is the one that catches a typo'd
id, which otherwise looks exactly like a task that is blocked forever.

## Step 3 — `crates/rivet-scheduler/src/local.rs`: detect cycles at submit

A client may name a dependency it has not submitted yet — it is building a graph
and the order is its own business. So keep

```rust
tasks: HashMap<TaskId, Vec<TaskId>>,   // every id ever submitted -> its deps
```

and on each `submit`, run a depth-first search from the new node. If you reach
the new node again, that submission closed a cycle: reject it and do not insert.
Because you only search from one node, this is O(V+E) per submit, not per graph.

Three colours, not a visited set: **white** unvisited, **grey** on the current
stack, **black** finished. Reaching grey is a cycle; reaching black is a
diamond, which is legal — look at `D` in the picture above. A plain `HashSet`
cannot tell those apart and will reject valid graphs.

An unresolved dependency is not an error yet, only an id you have not seen.
Check `UnknownDependency` when the task is *considered for dispatch*, not at
submit.

*Simpler alternative, if you want it:* require every dependency to be submitted
already. Then a dependency always has a lower id, a back-edge is impossible, and
cycles cannot occur by construction — no detection code at all. It is a real
design, it is what a topologically-ordered API gives you, and it costs the
client the freedom to submit in any order. Say in a comment which one you chose.

## Step 4 — `crates/rivet-scheduler/src/local.rs`: gate on eligibility

`schedule` currently hands `&mut self.pending` straight to the policy. Put the
gate in between:

1. Drain `pending` into two queues, asking of each task: is every id in
   `depends_on` present in `self.results` **with a successful result**?
2. Give the policy only the eligible queue.
3. Push the ineligible ones, plus whatever the policy did not place, back onto
   `pending`.

Watch the ordering in step 3. If blocked tasks always go to the front, a
long-blocked task at the head can keep starving newly eligible ones behind it;
if they always go to the back, a task's position drifts every tick. Preserving
submission order is the least surprising choice — say which you picked.

This is O(pending × deps) per tick. Fine at this scale. The index that removes
it is `dependents: HashMap<TaskId, Vec<TaskId>>` plus a per-task
`remaining_deps` counter, decremented as each dependency succeeds — Kahn's
algorithm, incrementally. Note it; do not build it yet.

## Step 5 — `crates/rivet-scheduler/src/local.rs`: cascade a permanent failure

In `worker_finished`, the branch that gives up after `max_retries` is where the
graph has to be told. A task whose dependency failed for good must be failed
too, with a result the client can actually read:

```rust
TaskResult::Failure { task_id, error: format!("dependency {dep} failed") }
```

Do it transitively — the dependents of the dependents fail as well — and remove
each cascaded task from `pending` as you go. A worklist over
`dependents` is the natural shape here, which is the first place Step 4's
reverse index actually earns its keep.

Retries are the case to be careful about: a dependency with attempts left has
*not* failed permanently, so nothing cascades. Only the final give-up does.

### Tests

| Test | File | Asserts |
|---|---|---|
| `a_task_with_no_dependencies_is_unaffected` | `rivet-scheduler/src/local.rs` | the whole M1–M5 suite still holds; `depends_on` empty means dispatch immediately |
| `a_blocked_task_is_not_dispatched` | `rivet-scheduler/src/local.rs` | B depends on A; with A unfinished, `schedule` returns A only |
| `a_task_runs_once_its_dependency_succeeds` | `rivet-scheduler/src/local.rs` | complete A, then B dispatches on the next `schedule` |
| `a_diamond_runs_in_topological_order` | `rivet-scheduler/src/local.rs` | A → {B, C} → D; D dispatches only after both B and C succeed, and the diamond is *not* mistaken for a cycle |
| `a_dependency_still_retrying_keeps_the_dependent_blocked` | `rivet-scheduler/src/local.rs` | A fails with attempts left; B is neither dispatched nor failed |
| `a_permanently_failed_dependency_fails_its_dependents` | `rivet-scheduler/src/local.rs` | A exhausts `max_retries`; B gets a `Failure` result and leaves `pending` |
| `a_failure_cascades_through_a_chain` | `rivet-scheduler/src/local.rs` | A ← B ← C; A fails for good, both B and C get results |
| `a_self_dependency_is_rejected` | `rivet-scheduler/src/local.rs` | a task depending on itself is `Err(DependencyCycle)` |
| `a_cycle_is_rejected_at_submit` | `rivet-scheduler/src/local.rs` | A→B→C→A: the third submit errors and the task is not stored |
| `a_diamond_is_accepted` | `rivet-scheduler/src/local.rs` | the false positive a `HashSet` gives you instead of three colours |
| `an_unknown_dependency_is_reported` | `rivet-scheduler/src/local.rs` | depending on an id never submitted surfaces `UnknownDependency` rather than blocking forever |
| `the_runtime_completes_a_dependency_chain` | `rivet-client/src/runtime.rs` | end to end: A → B → C through real workers, tick until done, all three succeed and C's result arrives last |

The last one is the only test that proves the whole path works. Bound its tick
loop — a cycle bug or a starvation bug both present as "never terminates", and
you want a failure message, not a hung suite.

### Questions to answer

1. What algorithm did you use for cycle detection, and what is its complexity
   in tasks and edges? Why is a two-state visited set not enough?
2. A dependency fails. Should the scheduler cancel the dependents, retry the
   dependency, or propagate the failure? What did you choose, and what would a
   CI system choose?
3. `depends_on` gives you a DAG per submission batch, but nothing stops two
   clients submitting into the same graph. What breaks first?

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
