# Blocking work (`mar::task::spawn_blocking`)

Source: [`src/task/blocking.rs`](../src/task/blocking.rs) (pool at [`src/task/worker_pool.rs`](../src/task/worker_pool.rs))

`spawn_blocking` lets user code run CPU-heavy or genuinely blocking work (file
I/O, a `reqwest` HTTP call, a long computation) without stalling the executor
thread. It is the runtime's one multi-threaded corner, and it's carefully
isolated.

## Two pieces

The module has two halves that work together:

1. **`WorkerPool`** — a set of worker threads that run closures sent over a
   channel.
2. **`BlockingTask`** — the future that offloads a closure to the pool and
   completes with its return value.

## `WorkerPool`

A fixed-size pool of threads (default one, `with_count` for more). Each worker
loops on `recv()`; when a job arrives it runs it and then calls a shared
`mio::Waker`:

```rust
// inside the worker loop
let job = rx.recv();
match job {
    Ok(job) => {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(job)); // never die
        let _ = w.wake();  // tell the executor: something finished
    }
    Err(_) => break,  // channel closed → exit
}
```

Two details matter:

- **Panic containment.** A panicking job must not kill a worker — a dead
  worker would silently strand every future job. Jobs run inside
  `catch_unwind`; the payload is delivered separately (see below).
- **Cross-thread wakeup.** The shared `mio::Waker` is created by `Mar::new`
  and registered with the reactor under `WAKEN_TOKEN` (`Token(0)`). When a
  worker calls `wake()`, `mio` fires that token in the executor thread, which
  then wakes every task in the blocking-waker map.

`Drop` closes the job channel and joins the workers — the runtime's shutdown
path. If any job sender leaked, `join()` would block forever.

## `BlockingTask` and `spawn_blocking`

`spawn_blocking(|| …)` is the ergonomic entry point:

1. Reads the shared state and job channel from the thread-local context
   (installed by `run()`).
2. Self-assigns a fresh blocking id from `RuntimeState::next_blocking_id`.
3. Creates an `mpsc` result channel.
4. Sends a closure to the pool that runs the user's closure inside
   `catch_unwind` and ships the outcome (`Result<R, panic payload>`) back over
   the channel.
5. Returns a `BlockingTask<R>` future.

When awaited, the future's poll:

- **First poll:** registers its waker in `RuntimeState::blocking_wakers`, then
  `try_recv()`s the result channel:
  - `Empty` → `Pending`. The executor parks; the worker is still running.
  - `Ok(Ok(value))` → remove the blocking entry, `Poll::Ready(value)`.
  - `Ok(Err(payload))` → remove the entry and `resume_unwind(payload)` —
    the panic crosses the thread boundary and is **rethrown on the executor
    thread**, inside the waiting task's poll, so `run()` panics with the
    original payload instead of hanging on a result that never comes.
  - `Disconnected` → panic loudly (worker died without sending).

### Cancellation

Dropping a `BlockingTask` before completion removes its entry from
`RuntimeState::blocking_wakers`. Without this, the termination check
(`blocking_wakers.is_empty()`) would fail forever and `run()` would park
forever.

## How the pieces fit together

```
user future ── spawn_blocking(|| …) ──> BlockingTask
                                            │ first poll
                                            ▼
                          RuntimeState::blocking_wakers ← waker registered
                                            │
                              worker thread runs closure
                                            │
                      result shipped via mpsc channel
                                            │
                      mio::Waker::wake()  ──┼──> WAKEN_TOKEN fires
                                            ▼
                         executor wakes every blocking waker
                                            │
                                            ▼
                         BlockingTask re-polled → try_recv Ok → Ready
```

The worker pool is created *eagerly* in `Mar::new` with the cross-thread waker
already wired, so `spawn_blocking` sends the job immediately — the closure
starts running before the calling future even yields. That eagerness is what
lets blocking work and timers interleave (see `tests/core.rs`).

## Key tests to read

- `worker_runs_a_closure` / `two_workers_run_jobs_concurrently` — pool basics,
  including proof that two workers overlap in wall-clock time.
- `workers_exit_when_pool_is_dropped` — shutdown joins cleanly.
- `spawn_blocking_interleaves_with_timer` (`tests/core.rs`) — the milestone
  test: a 200ms blocking job + a 50ms `sleep` finish in ~200ms, not 250ms.
- `panicking_blocking_closure_makes_run_panic` (`tests/core.rs`) — the panic
  payload crosses the boundary and is rethrown on the executor side.
- `dropped_spawn_blocking_leaves_blocking_map_empty` (in the executor module) —
  cancellation cleans up the blocking-waker map.
