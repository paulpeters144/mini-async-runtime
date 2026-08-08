# Plan B — replace the thread-local context with an explicit runtime handle

Goal: delete `src/context.rs` and the inline `ContextGuard` in `Mar::run`, and
make the data flow explicit by handing the root future a `Runtime` handle.

## 1. Why the guard exists today (the constraint)

`spawn_blocking` needs the pool's `job_tx` sender. Today that sender is cloned
into a thread-local `ContextHandle`, and `WorkerPool::drop` **joins** its
workers, which only exit when the job channel disconnects — i.e. when *every*
`Sender` is gone. The thread-local clone therefore outlives `run()` and pins
the channel open, so the join hangs. The guard exists only to release that
clone **before** `Mar` drops, on both the return and panic paths.

So the real invariant is:

> **No `job_tx` clone may outlive the pool.**

Any design that removes the guard must still honor this. It is an ordering
constraint, not a scoping one.

## 2. New public API

`Mar::run` takes a closure that receives an owned, cloneable handle:

```rust
use mar::Mar;

fn main() {
    Mar::run(|rt| async move {
        rt.sleep(Duration::from_millis(50)).await;
        let job = rt.spawn_blocking(|| heavy_work()).await;
        let [a, b] = mar::task::all([
            rt.spawn_blocking(|| 1u32),
            rt.spawn_blocking(|| 2u32),
        ])
        .await;
        println!("{job} {a} {b}");
    })
    .expect("run failed");
}
```

Signature:

```rust
pub fn run<F, Fut>(setup: F) -> io::Result<()>
where
    F: FnOnce(Runtime) -> Fut,
    Fut: Future<Output = ()> + 'static,
```

Decisions baked in:

- **Owned handle, not `&Runtime`.** `FnOnce(Runtime)` + `async move` keeps the
  future `'static`, which `Task::new` (and thus `spawn_root`) already requires.
  A `&Runtime` closure would force the root future to borrow a local and break
  that.
- **The handle is cloneable** (`Rc<RuntimeInner>`), so spawned work can keep a
  copy if we ever add public async `spawn`.

Removed free functions:

- `mar::time::sleep(...)` → `Runtime::sleep(&self, d: Duration) -> Sleep`
- `mar::task::spawn_blocking(...)` → `Runtime::spawn_blocking<F, R>(&self, f) -> BlockingTask<R>`
- `task::all(...)` stays as-is: it operates purely on futures, and the futures
  produced by `rt.sleep` / `rt.spawn_blocking` are owned (they clone the wheel /
  state), so `task::all([rt.spawn_blocking(a), rt.spawn_blocking(b)])` needs no
  borrow gymnastics.

## 3. The `Runtime` type

New `src/runtime.rs` (keeps `mar.rs` focused on the executor loop):

```rust
pub struct Runtime {
    pub(crate) inner: Rc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    state: Rc<RefCell<RuntimeState>>,
    wheel: Rc<TimerHeap>,
    job_tx: mpsc::Sender<Job>,
    completed_tx: mpsc::Sender<BlockingId>,
}

impl Runtime {
    pub(crate) fn new(state, wheel, job_tx, completed_tx) -> Self; // for run() and tests
    pub fn sleep(&self, duration: Duration) -> Sleep;
    pub fn spawn_blocking<F, R>(&self, f: F) -> BlockingTask<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}
```

It is just a **view**: it holds clones of the shared pieces `Mar` already owns.
`Sleep` and `BlockingTask` keep their current shapes — they capture only Rc
handles (`wheel` / `state`) at construction and never borrow the handle, so they
cannot outlive `run` by themselves.

Naming: `Runtime` is the recommendation; `Handle` (tokio-flavored) is the
alternative. Same structure either way.

## 4. Teardown and panic safety (the crux)

With the thread-local gone, the clone-ordering problem moves but does **not**
disappear. `spawn_root` stores the root future inside `runtime.state.tasks`, so
the handle (and its `job_tx` clone) lives inside `state`. `Mar` declares fields
in the order `state, wheel, reactor, pool, events`, so the pool drops **before**
state — i.e. before the task holding the sender clone is dropped. Naive
drop-order reliance would deadlock the same way the guard did.

The plan replaces the RAII guard with an **explicit, deterministic shutdown
sequence** inside `run`, wrapped in `catch_unwind` so the panic path is
identical to the happy path:

```rust
pub fn run<F, Fut>(setup: F) -> io::Result<()>
where
    F: FnOnce(Runtime) -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    let mut runtime = Self::new();
    let rt = Runtime::new(
        runtime.state.clone(),
        runtime.wheel.clone(),
        runtime.pool.job_tx(),
        runtime.pool.completed_tx(),
    );

    spawn_root(&runtime, setup(rt));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
        drain_ready_queue(&runtime);
        if is_done(&runtime) {
            return Ok(());
        }
        poll_readiness_events(&mut runtime)?;
        fire_due_timers(&runtime);
        wake_completed_blocking(&runtime);
    }));

    // Teardown runs on BOTH paths. Drop every task (releasing any Runtime
    // clones it holds) before closing the channel and joining workers.
    runtime.state.borrow_mut().tasks.clear();
    runtime.pool.shutdown();

    match result {
        Ok(ok) => ok,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
```

Supporting changes:

- `WorkerPool::shutdown()` — the single owner of teardown: drops `job_tx`,
  joins the workers. `Drop for WorkerPool` becomes a safety-net that does the
  same if `shutdown` was never called (idempotent).
- The `?` on `poll_readiness_events` inside `catch_unwind` propagates `Err` out
  of the closure (the closure returns `io::Result<()>`), so the teardown still
  runs before `Err` is returned.

Why this is better than the guard:

- No hidden scope: the ordering is explicit, readable, and covered by tests.
- No thread-local at all, so no per-thread state and no `install`/`uninstall`
  pairing to get wrong.
- Panic safety is a deliberate `catch_unwind` + `resume_unwind`, not a
  drop-order accident.

Rejected alternatives:

- **Reorder `Mar`'s fields so `state` drops before `pool`.** Works, but it is a
  fragile landmine — the exact class of hidden ordering bug the guard was
  patching.
- **Poll the root future as a local instead of storing it in `state.tasks`.**
  Bigger refactor of the task machinery; not worth it now.

Note for the future: if a public async `spawn` is ever added, spawned tasks may
hold `Runtime` clones. `tasks.clear()` already releases them, so the explicit
sequence above stays correct — keep it.

## 5. Files to change

| File | Change |
| --- | --- |
| `src/context.rs` | **Delete** (`ContextHandle`, `install`, `uninstall`, `with`). |
| `src/lib.rs` | Remove `pub mod context;`. Add `pub use` for `Runtime` alongside `Mar`. |
| `src/runtime.rs` | **New** — `Runtime` / `RuntimeInner` with `sleep`, `spawn_blocking`. |
| `src/mar.rs` | New `run` signature, build `Runtime`, remove guard + install, explicit teardown; update all `Mar::run(async …)` tests to `Mar::run(\|_rt\| async …)`; rework `dropped_spawn_blocking_leaves_blocking_map_empty` to build a `Runtime` directly (no hand-installed context). |
| `src/time.rs` | `sleep` becomes `Runtime::sleep`; delete the free fn + its `context::with`; fix doc comment; update the `sleep_zero_completes_in_runtime` test. |
| `src/task/blocking.rs` | `spawn_blocking` becomes `Runtime::spawn_blocking`; read `state`/`job_tx`/`completed_tx` from `self.inner` instead of `context::with`. |
| `src/task/mod.rs` | Remove `pub use blocking::spawn_blocking;`. |
| `src/task/worker_pool.rs` | Add `shutdown()`; make `Drop` idempotent; fix doc comments that mention the "thread-local context". |
| `tests/core.rs` | All four tests: closure form; the panic test now also covers the `catch_unwind` teardown path. |
| `src/task/all.rs` | Update `all_with_spawn_blocking` and `all_with_sleep_concurrent` tests. |
| `examples/*.rs` | All six: `Mar::run(\|rt\| async move { … })`. `fetch_to_file` has no sleep/spawn_blocking, so it becomes `Mar::run(\|_rt\| async move { … })`. |
| `docs/runtime-primitives.md` | Remove `ContextHandle` from the UML; add `Runtime` (and its `RuntimeInner` fields); draw `Mar --> Runtime` and `Runtime --> RuntimeState/TimerHeap`. |
| `README.md` | Update every API mention: `Mar::run(root_future)` → closure form, `time::sleep` / `task::spawn_blocking` → `rt.sleep` / `rt.spawn_blocking` (lines ~140, 169-172, 299-301, 475, 495-505, 541). |

## 6. Migration order (each step verifiable)

1. Add `src/runtime.rs` with `Runtime` / `RuntimeInner` (+ `pub(crate)::new` for
   tests). Wire `sleep` and `spawn_blocking` as methods. This compiles
   independently; the free fns still exist until step 3.
2. Rework `Mar::run` to the closure signature; build the `Runtime`; delete the
   context `install` and the `ContextGuard`.
3. Add `WorkerPool::shutdown()`; move teardown into `run` with `catch_unwind`.
4. Delete the free `sleep` / `spawn_blocking`; remove their re-exports and the
   `context::with` call sites.
5. Delete `src/context.rs`; drop `pub mod context;`.
6. Update all tests (`src/mar.rs`, `src/time.rs`, `src/task/all.rs`,
   `tests/core.rs`) to the closure form.
7. Update the six examples.
8. Update `docs/runtime-primitives.md` and `README.md`.
9. Verify: `cargo check`, `clippy`, `cargo test`, run all six examples.

## 7. New / changed tests

- `run` still panics with the blocking closure's payload
  (`panicking_blocking_closure_makes_run_panic`) — now exercises the
  `catch_unwind` teardown and proves workers are joined on the panic path.
- Add: a panicking **root** future still returns promptly and joins workers
  (panic-path analog of `runtime_drop_joins_workers_promptly_after_run`).
- `dropped_spawn_blocking_leaves_blocking_map_empty` — rework to construct a
  `Runtime` from hand-made `RuntimeState` / `TimerHeap` / `WorkerPool` instead
  of installing a context.
- Existing teardown test
  (`runtime_drop_joins_workers_promptly_after_run`, <500ms) stays green — it is
  the direct proof the sender ordering is correct.

## 8. Open decisions

1. Name: `Runtime` (recommended) vs `Handle`.
2. `Mar::run` closure keeps returning `io::Result<()>` (yes — no change).
3. Whether to keep `Sleep` / `BlockingTask` public constructors — they stay
   public types but are only constructible via `Runtime` (unchanged from today,
   since the free fns were the only constructors).

## 9. Out of scope

- Public async `spawn` (task spawn API). The teardown sequence is already
  correct for it; wiring the API is a separate feature.
- Reactor I/O support beyond the internal `mio` waker.
