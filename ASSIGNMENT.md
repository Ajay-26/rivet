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
