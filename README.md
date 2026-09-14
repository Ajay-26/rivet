# Rivet

A small distributed task-execution framework, built in Rust.

This is a systems programming project for two developers learning Rust. The
architecture and interfaces are provided — the interesting parts are yours to
implement.

---

## Overview

Rivet lets a client submit computational tasks that are distributed across a
pool of workers and executed concurrently. Think of it as a tiny version of
[Ray](https://docs.ray.io/) or a simplified distributed map-reduce.

The project is intentionally left unimplemented. Your job is to work through
the milestones below, filling in each `todo!()` as you go.

---

## Architecture

```
        ┌──────────────┐   submit / get_result
        │    Client    │ ──────────────┐
        └──────────────┘               │
        ┌──────────────┐               ▼
        │    Client    │ ────▶ ┌────────────────────┐
        └──────────────┘       │      Runtime       │
                               │ owns the scheduler │
                               │ and the pool       │
                               └─────────┬──────────┘
                                         │ asks: which worker?
                                         ▼
                               ┌────────────────────┐
                               │     Scheduler      │
                               │  (+ policy)        │
                               └─────────┬──────────┘
                                         │ assignments
                           ┌─────────────┼─────────────┐
                           ▼             ▼             ▼
                       ┌──────┐      ┌──────┐      ┌──────┐
                       │  W1  │      │  W2  │      │  W3  │
                       └──────┘      └──────┘      └──────┘
                              results ──▶ Runtime
```

Clients are thin handles. The **runtime** owns the scheduler and the worker pool
and moves tasks between them; the **scheduler** only decides placement and never
touches a worker. That split is what lets Milestone 6 move the runtime into
another process while the client barely changes.

### Component roles

| Crate | Role |
|---|---|
| `rivet-core` | Shared types: `Task`, `TaskId`, `TaskResult`, `WorkerInfo`, errors |
| `rivet-scheduler` | Decides which worker runs which task (`Scheduler` + `SchedulerPolicy`) |
| `rivet-worker` | Executes tasks and reports results |
| `rivet-client` | The runtime, the client API, and the CLI |

### Crate dependency graph

```
rivet-core
    ▲       ▲
    │       │
rivet-scheduler   rivet-worker
    ▲               ▲
    └───────┬────────┘
            │
      rivet-client
```

`rivet-scheduler` and `rivet-worker` only depend on `rivet-core` — they do not
depend on each other. This mirrors a real distributed system where the scheduler
and workers communicate over a network rather than through direct function calls.

---

## Quick start

```bash
cargo build          # should compile from day one
cargo test           # core-type tests pass; scheduler/worker tests fail until implemented
cargo run --bin rivet -- submit hello
```

---

## Milestones

### Milestone 1 — Local execution

**Goal:** tasks submitted to `LocalClient` are executed synchronously by a
`LocalWorker` in the same process.

- Implement `LocalScheduler::submit`, `schedule`, `worker_registered`,
  `worker_finished`.
- Implement `LocalWorker::execute` (any successful return is fine for now).
- Wire `LocalClient::tick()` to dispatch scheduled tasks to a worker.

**Tests to pass:** all tests in `rivet-scheduler` and `rivet-worker`, plus
`submit_returns_a_task_id`, `get_result_returns_none_for_pending_task` in the
integration test.

**Rust concepts:** structs, enums, `HashMap`, `VecDeque`, `match`, `Result`.

---

### Milestone 2 — Concurrent workers

**Goal:** multiple workers can execute tasks in parallel using OS threads.

- Spawn a thread per worker in `LocalWorker::execute`.
- Use channels (`std::sync::mpsc`) to send tasks to workers and receive results.
- `LocalClient::tick()` should drain the result channel and store finished results.

**Rust concepts:** `std::thread`, `std::sync::mpsc`, `Send`, ownership across
thread boundaries.

---

### Milestone 3 — A second scheduling policy

**Goal:** make worker selection pluggable and write a policy that provably
differs from first-available.

- Give `WorkerInfo` a `capacity` and an `in_flight` count; narrow
  `WorkerStatus` to liveness only.
- Extract selection into a `SchedulerPolicy` trait behind a `Box<dyn ...>`.
- Implement `FirstAvailablePolicy` and `LeastLoadedPolicy`, and test that they
  differ.

**Rust concepts:** trait objects, object safety, supertraits, `min_by_key`.

---

### Milestone 4 — Long-lived workers, and a runtime to own them

**Goal:** workers become threads you send to, and a new `Runtime` type takes
ownership of the scheduler and the pool so the client can shrink.

- Introduce `LocalRuntime` (`Arc<Mutex<..>>` over scheduler + worker pool +
  results channel). It spawns workers and drives `tick()`.
- `LocalClient` shrinks to a handle: `submit` and `get_result`, nothing else.
- One `mpsc` inbox per worker (the policy chooses between them); one shared
  results channel back to the runtime.
- `execute` takes `&self` so N threads can run on one worker.
- Shut down by dropping senders *then* joining — the reverse order hangs.

**Rust concepts:** `Arc`, `Mutex`, interior mutability, `mpsc::Sender`/`Receiver`,
`Send` vs `Sync`, `JoinHandle`, `Drop`.

---

### Milestone 5 — Fault tolerance

**Goal:** the system survives worker failures.

- Detect when a worker thread panics (hint: `JoinHandle::join` returns `Err`
  on panic; or use `std::panic::catch_unwind` inside the worker thread).
- Reschedule failed tasks on a different worker.
- Add a configurable retry limit.

**Rust concepts:** `Result` error propagation, `std::panic::catch_unwind`,
`Box<dyn Any + Send>`.

---

### Milestone 6 — Distributed execution

**Goal:** workers run as separate processes; the scheduler communicates with
them over TCP.

- Add a `wire` module to `rivet-core`: newline-delimited JSON, so framing is
  `BufReader::lines()` rather than something you hand-roll.
- Build a `rivet-worker` binary that binds a `TcpListener` and pumps tasks into
  the Milestone 4 thread pool it already has.
- Put `WorkerHandle` and a new `RemoteWorkerHandle` behind one
  `WorkerTransport` trait. A reader thread forwards results into the same
  `mpsc::Sender` the runtime already drains, so `tick()` does not change.
- Assign `WorkerId`s at the runtime, not in the worker — per-process counters
  collide.

**Rust concepts:** `serde`, `TcpListener`, `TcpStream`, `try_clone`, framing,
`AtomicBool`, trait objects across a transport boundary.

---

### Milestone 7 — Task graphs

**Goal:** tasks can declare dependencies on other tasks.

- The graph lives in the **scheduler**, not on `Task` and not in a policy.
  `Task.depends_on` only carries the list from client to scheduler, which then
  takes ownership of it — a worker never sees a dependency.
- Split *eligibility* (the scheduler's job — only it can see the results) from
  *placement* (the policy's job). A policy cannot answer the graph question
  anyway: `schedule` receives workers and pending tasks, never results.
- `Scheduler::submit` returns `Result<TaskId, _>` so a cycle can be reported.
- Detect cycles with a three-colour DFS — a plain visited set rejects diamonds.
- Cascade a permanent failure to its dependents, or they block forever.

```
A ──┬──> B ──┐
    │        ├──> D
    └──> C ──┘
```

**Rust concepts:** graph algorithms, topological sort, recursion with `Result`.

---

## Rust learning goals

The milestones are ordered so that you encounter Rust concepts naturally:

| Concept | First milestone |
|---|---|
| Structs, enums, `match` | 1 |
| `Result` and `?` operator | 1 |
| Traits and trait objects | 1 |
| `HashMap`, `VecDeque` | 1 |
| Ownership and borrowing | 1–2 |
| `std::thread` | 2 |
| Channels (`mpsc`) | 2–4 |
| `Arc`, `Mutex` | 4 |
| `Send` + `Sync` | 2–4 |
| Generics | 3–4 |
| Smart pointers (`Box`, `Rc`) | 4–5 |
| `std::panic::catch_unwind` | 5 |
| Async Rust (`async`/`await`) | 6 (optional) |
| Serialization (`serde`) | 6 |
| Lifetimes | 4–6 |
| Cargo workspaces | throughout |

---

## Dependencies

No external dependencies are used in the initial scaffold — only the Rust
standard library.

As you implement later milestones you may want to add:

| Crate | Purpose | Milestone |
|---|---|---|
| `serde` + `serde_json` / `bincode` | Task serialization | 6 |
| `tokio` | Async runtime | 6 (optional) |
| `uuid` | Distributed-unique IDs, if you'd rather not assign them centrally | 6 (optional) |
| `thiserror` | Less boilerplate on error enums | any |
| `tracing` | Structured logging | any |

Prefer the standard library for everything through Milestone 5.

---

## Code style

```bash
cargo fmt          # format all crates
cargo clippy       # lint all crates
cargo test         # run all tests
```

Lints are configured at the workspace level in the root `Cargo.toml`.
