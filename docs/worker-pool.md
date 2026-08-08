# Worker Pool: Running Expensive Code Without Freezing the Runtime

## Why Heavy Work Cannot Run on the Executor Thread

Everything in this runtime shares one thread. The executor loop, the task polling, the timer registry, the reactor — every part of the scheduling machinery runs on the same OS thread. This is the source of the runtime's simplicity, its speed, and its freedom from data races: there's only one place work happens, so there's nothing to synchronize. But it comes with a hard rule: the single thread must never be blocked.

A computation that runs for two hundred milliseconds on the executor thread doesn't just delay one task — it freezes every task in the system for two hundred milliseconds. Timers can't fire because nobody is checking them. The reactor can't process I/O events because nobody is parked inside the kernel. Worker threads can finish jobs and signal the pool, but nobody is listening to the completion channel. The thread isn't polling, it isn't waiting efficiently, it isn't doing any of the things that make the async model work. It's just running synchronous code, blind to everything else, and cooperative scheduling gives nobody the power to interrupt it.

Some work cannot be broken into async pieces. CPU-heavy loops, synchronous file I/O that lacks an async API, FFI calls into C libraries that block internally — these run start-to-finish before returning control to the caller. There's no `.await` to insert to yield the thread back, because the function doesn't return until it's done. This work cannot be made cooperative, which means it cannot run on the executor thread without freezing everything else. It needs somewhere else to go — a different thread, one whose only job is to run the slow stuff and report back.

## The Solution: A Fixed Set of Helper Threads

A small pool of dedicated OS threads whose only job is to run blocking closures. The executor never runs a blocking closure itself. Instead, it hands the closure to the pool through a channel, and the pool runs it on a different thread. The executor continues polling other tasks while the work runs elsewhere.

When the closure finishes, the pool notifies the executor, which wakes the task that was waiting for the result. To the awaiting task, `spawn_blocking(f).await` looks like any other async operation — it returns `Pending`, the runtime moves on, and eventually the task resumes with the result.

## The Struct

```rust
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct WorkerPool {
    job_tx: Option<mpsc::Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
    completed_tx: mpsc::Sender<BlockingId>,
    completed_rx: mpsc::Receiver<BlockingId>,
}
```

**`Job`** — a type-erased closure. `FnOnce` means it runs once and consumes its captures (the closure is destroyed after execution). `Send` means it can move to another thread. `'static` means it owns everything it references — no borrowing from the executor's stack, because the executor might move on while the closure is still running.

**`job_tx: Option<mpsc::Sender<Job>>`** — the channel for sending closures from the executor to the workers. One producer (the executor), N consumers (the workers). The `Option` exists so shutdown can `take` the sender out of the struct to close the channel.

**`workers: Vec<JoinHandle<()>>`** — one handle per worker thread. `join()` on a handle blocks until that thread exits.

**`completed_tx` and `completed_rx`** — the channel for workers to tell the executor which jobs finished. N producers (workers), one consumer (the executor). Workers send `BlockingId`s through this channel; the executor drains them and wakes the corresponding tasks.

Notice: this is *two* channels, not one. The job channel carries closures one way. The completed channel carries ids the other way. They serve different producers, different consumers, and different data types.

## Starting the Pool

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

    WorkerPool { job_tx: Some(job_tx), workers, completed_tx, completed_rx }
}
```

Each worker thread runs the same loop forever:

1. **Wait for a job.** `recv()` blocks until a closure arrives on the job channel.
2. **Run it.** The closure is executed inside `catch_unwind` — if the closure panics, the worker catches it and keeps running. A dead worker permanently reduces capacity.
3. **Wake the executor.** After every job (successful or panicked), the worker calls `w.wake()` on the shared `mio::Waker`. This unblocks the executor's parked `mio::Poll::poll` so it can process the result.
4. **Exit on close.** When `recv()` returns `Err`, the job channel has been closed (the runtime is shutting down). The worker exits the loop.

The `job_rx` receiver is wrapped in `Arc<Mutex<...>>` because `mpsc::Receiver` isn't `Clone`, but N workers must share one receiver. The `Mutex` serializes access: only one worker waits on the receiver at a time. This is safe because the channel is unbounded (so `send` never blocks) and `recv()` blocks while holding the lock — which is fine, since the lock isn't needed by anyone else while the worker waits.

## How `spawn_blocking` Builds the Bridge

Here's the function that turns a synchronous closure into an async future:

```rust
pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (state, job_tx, completed_tx) = context::with(|ctx| {
        (ctx.state.clone(), ctx.job_tx.clone(), ctx.completed_tx.clone())
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

    BlockingTask { state, id: task_id, rx: Some(rx), done: false }
}
```

Let's walk through what's happening:

1. **Read the context.** Clone the runtime's state, job sender, and completed-id sender from the thread-local context.
2. **Allocate a `BlockingId`.** Bump the counter, get a unique id for this job.
3. **Create a per-job result channel.** `mpsc::channel::<Result<R>>()`. This private channel connects the worker (which sends the result) to the `BlockingTask` future (which receives it). Each job gets its own channel.
4. **Build the closure.** The closure captures the user's function `f`, the result sender `tx`, and the completed-id sender `completed_tx`. When the worker runs it: it calls `catch_unwind(f)`, sends the result through the private channel, and sends the `BlockingId` through the completed channel so the executor knows which task to wake.
5. **Dispatch.** Send the closure through the job channel to the pool.
6. **Return the future.** `BlockingTask` is the future the caller awaits.

## The `BlockingTask` Future

```rust
pub struct BlockingTask<R> {
    state: Rc<RefCell<RuntimeState>>,
    id: BlockingId,
    rx: Option<mpsc::Receiver<std::thread::Result<R>>>,
    done: bool,
}
```

It holds the runtime state (so it can register and remove its waker), the blocking id (so the executor can find it), the receiving end of the per-job result channel, and a `done` flag.

### `BlockingTask::poll`

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
    let this = self.get_mut();

    // Refresh the waker — the executor might give a different one.
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
        Err(TryRecvError::Empty) => Poll::Pending,
        Err(TryRecvError::Disconnected) => {
            panic!("worker thread disconnected without sending a result");
        }
    }
}
```

Every poll does two things:

First, it registers (or refreshes) a waker in `blocking_wakers` under its `BlockingId`. This tells the executor "when job N finishes, wake *this* task." The `and_modify().or_insert()` pattern refreshes the waker in place if it already exists, or inserts a new one if this is the first poll.

Then it checks the result channel with `try_recv()` (non-blocking):

- **`Ok(Ok(result))`** — the closure returned a value. Clean up the waker entry and return `Ready(result)`.
- **`Ok(Err(payload))`** — the closure panicked. Clean up the entry and re-raise the panic on the executor thread via `resume_unwind`. This surfaces the panic in the task that awaited the `BlockingTask`, not in the worker.
- **`Err(Empty)`** — the worker hasn't finished yet. Return `Pending`.
- **`Err(Disconnected)`** — the worker died without sending a result. This should never happen (workers are protected by `catch_unwind`), so it panics.

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

If the task is dropped before the job finishes — the future was cancelled, or the runtime is shutting down — the waker entry must be removed from `blocking_wakers`. A stale entry blocks `is_done` forever, and `run()` hangs.

## The Executor Side: Processing Completions

When the reactor returns with `WAKEN_TOKEN`, the executor calls `wake_completed_blocking`:

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

For each completed job: look up the `BlockingId` in `blocking_wakers`, find the waker for the task that's awaiting it, and call `wake_by_ref()`. This pushes the *task's* id onto the ready queue.

Note that waking does *not* remove the entry from the map. That's the `BlockingTask`'s responsibility — when it gets polled and finds the result, it removes its own entry. Waking is a signal; cleanup is the recipient's job.

## Walking Through a Full Cycle

Let's follow `task::spawn_blocking(|| 21 * 2).await` inside `Mar::run`. Root task is `TaskId(0)`. Blocking id counter starts at `BlockingId(0)`.

**1. `spawn_blocking` is called.** It reads the context, allocates `BlockingId(0)` (counter becomes `BlockingId(1)`), creates a per-job result channel, wraps `|| 21 * 2` in a job closure, and sends it to the pool. It returns `BlockingTask { id: BlockingId(0), ... }`.

**2. The awaiting future polls the `BlockingTask`.** `BlockingTask::poll` registers the waker under `BlockingId(0)` in `blocking_wakers`. `try_recv()` returns `Empty` — the worker hasn't started yet. Returns `Pending`. Task parks.

**3. A worker picks up the job.** It receives the closure, runs `catch_unwind(|| 21 * 2)` → `Ok(42)`. Sends `Ok(42)` through the per-job result channel. Sends `BlockingId(0)` through the completed channel. Calls `wake()` on the `mio::Waker`.

**4. The executor wakes.** The reactor returns with `WAKEN_TOKEN`. `wake_completed_blocking` drains `[BlockingId(0)]`, finds the waker under that key, and calls `wake_by_ref()`. `TaskId(0)` is pushed onto the ready queue.

**5. The task is polled again.** `BlockingTask::poll` refreshes the waker, then `try_recv()` returns `Ok(Ok(42))`. It removes the `BlockingId(0)` entry from `blocking_wakers`, sets `done = true`, returns `Ready(42)`. The task completes. Tables are empty. `run()` returns.

## Shutdown

When `Mar::run` finishes and `runtime` drops:

```rust
pub(crate) fn shutdown(&mut self) {
    drop(self.job_tx.take());
    for handle in self.workers.drain(..) {
        let _ = handle.join();
    }
}
```

First, take the job sender out and drop it. The channel closes. Every worker's `recv()` returns `Err` (because the channel is closed and empty). Each worker breaks out of its loop and the thread exits. Then `join()` waits for each thread to finish.

The `Option` wrapper on `job_tx` is necessary because `Drop` can't move a value out of a struct field without wrapping it. `take()` swaps in `None` and returns `Some(sender)`, which is then immediately dropped.

This is also why the `ContextGuard` must drop *before* the runtime. The guard holds a clone of `job_tx`. If the guard were still alive when the pool tries to close the channel, the channel would have a live sender and workers would block forever in `recv()`.

## Design Choices

**Why two channels?** The job channel carries closures: one producer, many consumers. The completed channel carries ids: many producers, one consumer. They're different directions, different types, and different purposes. Merging them would force every worker to consume every other worker's results — wasteful and confusing.

**Why `catch_unwind` in the worker loop?** A panicking closure must not kill the worker. A dead worker permanently reduces capacity. The worker catches the panic, the payload gets shipped back through the per-job result channel, and `resume_unwind` re-raises it on the executor thread where the awaiting task can handle it.

**Why `mio::Waker` and not another channel?** The executor is parked inside `mio::Poll::poll`. A message pushed to a regular channel would sit in memory with nobody reading it. Only a registered wake source (like `mio::Waker`) can unblock the executor from inside the kernel.

**Why `Arc<Mutex<Receiver>>` for the job channel?** The receiver isn't `Clone`, but all workers must share it. The `Mutex` serializes access, and since `recv()` blocks while holding the lock, only one worker waits at a time. The lock is never contended — a worker releases it only to do actual work.

## Common Misconceptions

**"Workers poll futures."** Workers run closures only. They have no `Future`, no `Context`, no `Waker`. They call a `FnOnce` and send the result back. The executor is the only thing that polls.

**"The worker wakes the blocked task directly."** The worker wakes the *executor* via `mio::Waker`. The executor then looks up the `BlockingId` in `blocking_wakers` and wakes the specific task. The worker has no idea which task is waiting.

**"The completed channel carries results."** It carries `BlockingId`s — just numbers that say "job N is done." The actual result travels through the per-job result channel, which is private to each `BlockingTask` future.

**"A panicking closure swallows the panic."** The panic payload is shipped through the per-job result channel and re-raised on the executor thread via `resume_unwind`. The panic surfaces in the task that awaited the `BlockingTask`, exactly where it is expected.

**"Spawning a blocking task wakes every blocked task."** Each completion wakes exactly one task — the one whose `BlockingId` matches. The lookup in `blocking_wakers` is precise.

## Summary

The `WorkerPool` runs blocking closures on a fixed set of OS threads so the executor thread stays responsive. Two channels carry work in opposite directions: closures from executor to workers, completion ids from workers back to executor. `BlockingTask` bridges sync and async: it wraps the per-job result channel and registers a waker so the executor knows which task to wake when the job finishes. Its `Drop` removes the waker entry to prevent the runtime from hanging if the task is cancelled. And the `ContextGuard`'s drop order — before the pool — is the detail that makes clean shutdown possible.

Source: `src/task/worker_pool.rs`, `src/task/blocking.rs`
