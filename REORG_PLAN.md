# Reorganization Plan

**Goal:** Better module names and organization so the runtime reads as a clear educational piece.
End state mirrors tokio's public API: `mar::time::sleep`, `mar::task::spawn_blocking`, `mar::task::yield_now`, `mar::io::read` / `write`.

---

## Status

- [x] **Phase 1 — Single thread-local context** (`src/context.rs`)
  One TLS holding `state`, `reactor`, `wheel`, `job_tx`, `current_id`.
  Migrated all three old install paths into it. Kept `current_id` alive.
  Build green, 38 tests pass, clippy clean.

- [x] **Phase 2 — Waker-based timer heap + rename `timer_wheel` → `time`**
  New `TimerHeap` struct with internal `RefCell<BinaryHeap<Reverse<TimerEntry>>>`
  and `next_id`. Entries hold `(deadline, id, Waker)` so `expire_due` wakes
  tasks directly via `wake_by_ref()`. `sleep()` no longer needs `current_id`.
  Renamed file to `time.rs`. Deleted `src/timer_wheel.rs`.
  Updated `lib.rs`, `mar.rs`, `context.rs`, examples, tests.

- [x] **Phase 3 — Kill `current_id` entirely**
  Added `next_blocking_id` counter to `RuntimeState`.
  `spawn_blocking` self-assigns ids from the counter.
  Deleted `set_current_id` / `clear_current_id` / `current_id` from
  `context.rs` and removed the two hot-loop calls in the executor.

- [x] **Phase 4 — `src/task/` directory**
  ```
  src/task/
    mod.rs          — Task struct + subsystem docs + re-exports
    yield_now.rs    — YieldNow + yield_now()
    blocking.rs     — BlockingTask + spawn_blocking
    worker_pool.rs  — WorkerPool + Job
  ```
  Deleted `src/task.rs`, `src/blocking.rs`, `src/worker_pool.rs`.
  `waker.rs` stays standalone at root.
  Public API: `mar::task::spawn_blocking`, `mar::task::yield_now`.

- [x] **Phase 5 — Naming pass**
  `Readable` → `ReadFuture`, `Writable` → `WriteFuture` in `io.rs`.
  `state.blocking` → `state.blocking_wakers` throughout the crate.

- [x] **Phase 6 — Update docs**
  Renamed `docs/timer-wheel.md` → `docs/time.md`.
  Updated `docs/task.md`, `docs/blocking.md`, `docs/io.md`, `docs/waker.md`,
  `docs/reactor.md`, `docs/executor.md`, `docs/runtime-state.md`.
  Updated README links table, repository layout, and component descriptions.

- [x] **Final verification**
  `cargo build` clean, `cargo test` 38 passed, `cargo clippy --all-targets` clean.

---

## Decisions made

- Waker stays at `src/waker.rs` (standalone, not inside `task/`).
- Thread-local module named `context.rs`.
- IO futures renamed `ReadFuture` / `WriteFuture`.
