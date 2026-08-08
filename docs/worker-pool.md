# Worker Pool: Offloading Blocking Work

## 1. Concept: Why a Thread Pool?

### The Core Concept

A fixed set of OS threads runs blocking closures off the executor thread, so a long computation or a blocking call freezes nothing else in the runtime. The rest of the section is the deep dive.

### The Problem

Some work cannot be made async. Synchronous file I/O, CPU-heavy computation, and blocking C libraries all run to completion before returning control. If that work runs on the executor thread, a 200ms computation freezes every other task, every timer, and all I/O for 200ms. The executor thread is the runtime's only thread; anything that blocks it blocks the entire system.

### The Solution

A fixed set of worker threads that run blocking closures, and channels that carry closures from the executor to the workers and results back. The executor never runs a blocking closure itself; it hands the closure to the pool and waits for the result asynchronously.

### How `spawn_blocking` Bridges Sync and Async

`task::spawn_blocking(f)` returns a future. The first poll submits the closure to the pool and returns `Pending`; a later poll — after a worker finished — reads the result and returns `Ready(value)`. To the awaiting task it looks like any other async operation. The bridge works because each job gets its own result channel: the worker sends the value through the channel, and the `BlockingTask` future reads it on the executor thread.

### Why a Fixed-Size Pool

Thread creation is expensive — a kernel thread allocates a stack and registers with the OS scheduler — so threads are created once and reused. A fixed size bounds memory usage and OS-thread count. This runtime uses 3 workers by default.

## 2. How This Runtime Implements WorkerPool

### The Type Alias and Struct

The pool lives in `src/task/worker_pool.rs`:

```rust
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct WorkerPool {
    job_tx: Option<mpsc::Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
    completed_tx: mpsc::Sender<BlockingId>,
    completed_rx: mpsc::Receiver<BlockingId>,
}
```

### Field by Field

**`job_tx: Option<mpsc::Sender<Job>>`.** The executor→workers channel. The `Option` exists so `shutdown` can take the sender out of the struct during `Drop`; you cannot move out of a field in `Drop` without wrapping it. Taking it closes the channel, which causes every worker's `recv()` to return `Err` and exit.

**`workers: Vec<JoinHandle<()>>`.** One `JoinHandle` per worker thread. `JoinHandle` is the std type that represents a running thread; `join()` blocks until the thread exits and returns its result.

**`completed_tx: mpsc::Sender<BlockingId>`.** Workers→executor channel, sender side. Each worker sends the `BlockingId` of the job it just finished, so the executor knows which task to wake.

**`completed_rx: mpsc::Receiver<BlockingId>`.** The receiver side, kept on the pool. The executor calls `drain_completed()` to pull all pending ids.

### `Job`

`Box<dyn FnOnce() + Send + 'static>` is a type-erased closure. `FnOnce` means it runs once and consumes its captures. `Send` means it can be moved to another thread. `'static` means it owns everything it references — no borrowed data from the executor's stack. Return values do not travel through the `Job` itself; each job gets its own per-job result channel (see `spawn_blocking`).

### Construction

`with_count` in `src/task/worker_pool.rs`:

```rust
pub fn with_count(waker: Arc<mio::Waker>, n: usize) -> Self {
    let (job_tx, job_rx) = mpsc::channel::<Job>();
    let (completed_tx, completed_rx) = mpsc::channel();
    let job_rx = Arc::new(Mutex::new(job_rx));
    let mut workers = Vec::with_capacity(n);

    for _ in 0..n {
        let rx = job_rx.clone();
        let w = waker.clone();
        workers.push(thread::spawn(move || {
            loop {
                let job = rx.lock().unwrap().recv();
                match job {
                    Ok(job) => {
                        let _ = std::panic::catch_unwind(
                            std::panic::AssertUnwindSafe(job),
                        );
                        let _ = w.wake();
                    }
                    Err(_) => break,
                }
            }
        }));
    }

    WorkerPool {
        job_tx: Some(job_tx),
        workers,
        completed_tx,
        completed_rx,
    }
}
```

Line by line. The job channel is created with `mpsc::channel()`. The completed channel is created the same way. The `job_rx` receiver is wrapped in `Arc<Mutex<_>>` because `mpsc::Receiver` is not `Clone`, but N workers must share one receiver; the `Mutex` serializes access. Each thread gets a clone of the shared receiver and a clone of the `mio::Waker`.

### The Worker Loop

Each worker runs this loop:

```rust
let job = rx.lock().unwrap().recv();
match job {
    Ok(job) => {
        let _ = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(job),
        );
        let _ = w.wake();
    }
    Err(_) => break,
}
```

`recv()` blocks until a job arrives. The closure runs inside `catch_unwind` — defense-in-depth against panics. After every job the worker calls `w.wake()` on the `mio::Waker` to wake the executor. The wake is unconditional: even if the job panicked, the worker still wakes the executor so it can observe the result (or the panic payload). On `Err` (channel closed) the worker exits the loop.

### Why `w.wake()` and Not a Channel

The executor is parked inside `mio::Poll::poll`. A message pushed to a channel would sit in memory because nothing is reading it. The `mio::Waker` is registered with the OS poller, so `wake()` causes the kernel to unblock the parked thread. It is the only cross-thread→main-thread wake path.

### Shutdown

```rust
pub(crate) fn shutdown(&mut self) {
    drop(self.job_tx.take());
    for handle in self.workers.drain(..) {
        let _ = handle.join();
    }
}
```

`drop(self.job_tx.take())` takes the sender out of the `Option` and drops it, closing the channel. Every worker's `recv()` returns `Err`, the `break` fires, and the thread exits. The loop then joins every handle. `Drop` delegates to `shutdown()` so the teardown order lives in one place and is idempotent (the `Option` is `None` on the second call).

### `spawn_blocking`

In `src/task/blocking.rs`:

```rust
pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (state, job_tx, completed_tx) = context::with(|ctx| {
        (
            ctx.state.clone(),
            ctx.job_tx.clone(),
            ctx.completed_tx.clone(),
        )
    });

    let task_id = {
        let mut state = state.borrow_mut();
        let id = state.next_blocking_id;
        state.next_blocking_id.0 += 1;
        id
    };

    let (tx, rx) = mpsc::channel::<std::thread::Result<R>>();
    let job: Job = Box::new(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let _ = tx.send(result);
        let _ = completed_tx.send(task_id);
    });
    job_tx.send(job).expect("worker pool is shut down");
    BlockingTask {
        state,
        id: task_id,
        rx: Some(rx),
        done: false,
    }
}
```

Line by line. `context::with` reads the thread-local runtime handle and clones the state, job sender, and completed-id sender. A `BlockingId` is allocated from `next_blocking_id` (monotonically increasing, never reused). A per-job result channel `mpsc::channel::<std::thread::Result<R>>()` is created; it carries the closure's value *or* its panic payload. The job closure wraps the user closure in `catch_unwind`, sends the result through the per-job channel, then sends the `BlockingId` through the completed channel. The job is dispatched to the pool. The `BlockingTask` future is returned to the caller.

### `BlockingTask::poll`

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
    let this = self.get_mut();
    if this.done {
        unreachable!("BlockingTask polled after completion");
    }

    this.state
        .borrow_mut()
        .blocking_wakers
        .entry(this.id)
        .and_modify(|existing| existing.clone_from(cx.waker()))
        .or_insert_with(|| cx.waker().clone());

    match this.rx.as_mut().unwrap().try_recv() {
        Ok(Ok(result)) => {
            this.done = true;
            this.state.borrow_mut().blocking_wakers.remove(&this.id);
            Poll::Ready(result)
        }
        Ok(Err(payload)) => {
            this.done = true;
            this.state.borrow_mut().blocking_wakers.remove(&this.id);
            std::panic::resume_unwind(payload);
        }
        Err(mpsc::TryRecvError::Empty) => Poll::Pending,
        Err(mpsc::TryRecvError::Disconnected) => {
            panic!("worker thread disconnected without sending a result");
        }
    }
}
```

The waker upsert runs on every poll: `entry().and_modify().or_insert()` refreshes the stored waker in place (the task may be re-scheduled with a different waker) or inserts a new one. Then `try_recv` checks the per-job channel: `Ok(Ok(r))` means the closure returned a value — remove the entry and return `Ready`. `Ok(Err(payload))` means the closure panicked — remove the entry and `resume_unwind` the payload on the executor thread, inside the awaiting task's poll. `Err(Empty)` means the worker has not finished yet — return `Pending`. `Err(Disconnected)` means the worker died without sending a result — panic.

### `BlockingTask::Drop`

```rust
impl<R> Drop for BlockingTask<R> {
    fn drop(&mut self) {
        if !self.done {
            self.state.borrow_mut().blocking_wakers.remove(&self.id);
        }
    }
}
```

If the task is dropped before completion (cancelled), the waker entry must be removed. A stale entry in `blocking_wakers` blocks `is_done` forever — the runtime's termination check requires that map to be empty.

### Executor Side: `wake_completed_blocking`

In `src/mar.rs`:

```rust
fn wake_completed_blocking(runtime: &Mar) {
    for event in &runtime.events {
        if event.token() == WAKEN_TOKEN {
            let completed = runtime.pool.drain_completed();
            for completed_id in completed {
                let waker = {
                    let state = runtime.state.borrow();
                    state.blocking_wakers.get(&completed_id).cloned()
                };
                if let Some(w) = waker {
                    w.wake_by_ref();
                }
            }
        }
    }
}
```

When the reactor returns with `WAKEN_TOKEN`, the executor drains all pending completed ids. For each id it looks up the corresponding waker in `blocking_wakers` and calls `wake_by_ref()`, which pushes the *task's* id onto the ready queue. The split responsibility is important: waking does not remove the entry; the entry is removed by `BlockingTask::poll` when it observes the result, or by its `Drop` if cancelled.

### Worked Example

Trace `task::spawn_blocking(|| 21 * 2).await` inside `Mar::run`. The root task is `TaskId(0)`. The blocking id counter starts at `BlockingId(0)`.

1. **First poll.** `spawn_blocking` reads the context, allocates `BlockingId(0)`, creates a per-job result channel `(tx, rx)`. The job closure captures `f`, `tx`, and `completed_tx`. The job is sent to the pool. A `BlockingTask` is returned. The awaiting future polls it: `BlockingTask::poll` inserts the waker under `BlockingId(0)` in `blocking_wakers`, then `try_recv` returns `Err(Empty)` — `Pending`. The task's id is re-queued by its own waker.

2. **Worker receives.** A worker thread locks the shared receiver, calls `recv()`, gets the job. It runs `catch_unwind(f)`: `f` returns `42`, so `catch_unwind` returns `Ok(42)`. The worker sends `Ok(42)` on `tx`, then sends `BlockingId(0)` on `completed_tx`. It calls `w.wake()`.

3. **Executor wakes.** The `mio::Waker::wake()` call unblocks the parked `mio::Poll::poll`. The reactor returns with `WAKEN_TOKEN`. `wake_completed_blocking` drains `[BlockingId(0)]`, looks up the waker stored under `BlockingId(0)` in `blocking_wakers`, and calls `wake_by_ref()`. This pushes `TaskId(0)` onto the ready queue.

4. **Second poll.** The drain pops `TaskId(0)`, polls the task. `BlockingTask::poll` refreshes the waker entry, then `try_recv` returns `Ok(Ok(42))`. The entry is removed from `blocking_wakers`, `done` is set to `true`, and `Ready(42)` is returned. The future completes; the task drops.

State after: task table `{}`, `blocking_wakers = {}`, queue `[]`.

### Source Links

- `src/task/worker_pool.rs` — the `WorkerPool` struct, construction, worker loop, shutdown
- `src/task/blocking.rs` — `spawn_blocking`, `BlockingTask`, its `poll` and `Drop`
- `src/mar.rs` — `wake_completed_blocking`, `Mar::new`

## 3. Design Decisions and Tradeoffs

**Why two channels instead of one.** The job channel carries closures: one producer (the executor), N consumers (workers). The completed channel carries `BlockingId`s: N producers (workers), one consumer (the executor). Reusing one channel would mean every worker consuming every other worker's completion and broadcasting results. The second channel lets the executor wake only the task that finished. Broadcast-waking would be correct but wasteful — every blocked task would re-poll and re-find nothing.

**Why `catch_unwind`.** A panicking job must not kill a worker. A dead worker permanently reduces capacity and strands future jobs. The payload is shipped back through the per-job result channel and re-raised on the executor thread via `resume_unwind`, so the panic surfaces in the task that awaited it, not in the worker.

**Why `Option<Sender<Job>>`.** `Drop` must take the sender out to close the channel. Without `Option` the field cannot be moved in `Drop`; the `Option` lets `shutdown` call `self.job_tx.take()`.

**Why the worker wakes the executor via `mio::Waker` instead of a channel.** The executor is blocked inside `mio::Poll::poll`; only a registered wake source can unblock it. A message pushed to a channel would sit in memory with nobody reading it.

**Why the shared `job_rx` is wrapped in `Arc<Mutex<...>>`.** `mpsc::Receiver` is not `Clone`, but N workers must share one receiver. The `Mutex` serializes access; `recv()` blocks while holding the lock, which is safe because only one worker waits at a time and the channel is unbounded (so `send` never blocks).

**What `BlockingId` maps to.** The waker stored in `RuntimeState::blocking_wakers` under that key. The mapping is how "a job finished" becomes "poll exactly this task".

**When this changes.** A multi-threaded executor would give each thread its own pool or use work-stealing. Dynamic sizing, job cancellation, or per-job timeouts would change the design.

## 4. Failure Modes and Misconceptions

### What Breaks If Implemented Wrong

**A worker thread panicked without the `catch_unwind` guard.** The worker dies mid-loop, its `JoinHandle` is never joined cleanly, and every future job sent to that worker's slot is stranded — the runtime would hang. Both catch layers exist: the worker's `catch_unwind` (defense-in-depth) and the job closure's `catch_unwind` (ships the payload back).

**A `BlockingTask` is dropped without removing its waker.** The entry stays in `blocking_wakers`; `is_done` never sees an empty map; `run()` never returns. The test `dropped_spawn_blocking_leaves_blocking_map_empty` guards this: it polls a `BlockingTask`, drops it, and asserts the map is empty.

**A sender to the job channel survives `shutdown`.** If the thread-local context still holds a clone of `job_tx` when `Drop` runs, `recv()` never returns `Err`, and `join()` blocks forever. This is why the `ContextGuard` must be declared *after* the runtime in `Mar::run`: locals drop in reverse order, so the guard drops first, releasing the sender clone before the pool joins.

### Common Misunderstandings

**"Workers poll futures."** Workers run closures only. They have no `Future` trait machinery, no `Context`, no `Waker`. They run a `FnOnce` and send the result back.

**"The worker wakes the blocked task."** The worker wakes the *executor*, via `mio::Waker`. The executor then wakes the specific task via the `BlockingId` lookup in `blocking_wakers`. The worker does not know which task is waiting.

**"The completed channel carries results."** It carries `BlockingId`s. The result channel is per-job and private to the `BlockingTask` future. The two channels serve different purposes: the completed channel is the "something finished" signal; the result channel carries the actual value.

**"A panicking closure is swallowed."** Its payload is shipped through the per-job result channel and re-raised on the executor thread via `resume_unwind`. The panic surfaces in the task that awaited the `BlockingTask`.

**"Each job wakes every blocked task."** Each job's completion wakes only its own task. The `BlockingId` lookup in `blocking_wakers` is precise — it finds the exact waker for the task that submitted the job.

## 5. Summary

- The `WorkerPool` runs blocking closures on a fixed set of OS threads so the executor thread is never blocked.
- `Job` is a type-erased `Box<dyn FnOnce()>`; each job gets a per-job result channel for its value or panic payload.
- Two channels: the job channel (executor→workers) and the completed channel (workers→executor, carrying `BlockingId`s).
- Workers wake the executor via `mio::Waker`, which unblocks the parked `mio::Poll::poll`.
- `BlockingTask` bridges sync and async: first poll submits, later poll reads the result.
- `BlockingTask::Drop` removes the waker entry to prevent `is_done` from hanging.
- The `ContextGuard` must drop before the pool to prevent a join deadlock.
