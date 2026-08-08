# Mar: The Single-Threaded Executor and Its Event Loop

## 1. Concept: What Is an Async Executor?

### The Core Concept

The executor is the component that drives futures to completion. Rust gives you `Future`, `Pin`, and `Waker`, but no mechanism that calls `poll`. The executor is that mechanism: it owns tasks, polls them in some order, and repeats until every task is done. Without it, a future is a state machine that never advances.

Every executor is a loop: poll everything that is ready; when nothing is ready, wait efficiently; when something becomes ready — an I/O event, a timer, a blocking job completion — poll again; stop when no work exists anywhere. The user's `async` code runs *inside* the executor's `poll` call.

A `Context` is a standard-library struct that carries one piece of data: the `Waker`. The executor builds a `Context` from the task's waker and passes it to `poll`, so the future can clone the waker and hand it to whatever external driver will eventually signal readiness. `Poll` is the enum `poll` returns:

```rust
pub enum Poll<T> {
    Ready(T),
    Pending,
}
```

`Ready(T)` means "done, here is the value"; `Pending` means "not done, poll me again later". The executor inspects this to decide whether to drop the task or park it.

### The Problem

Here is a future that tries to drive another future without an executor:

```rust
let waker = Waker::noop();
let mut cx = Context::from_waker(&waker);
let mut fut = async { some_async_work().await; };

match fut.poll(&mut cx) {
    Poll::Ready(value) => { /* done */ }
    Poll::Pending => {
        // Nobody holds the waker. Nobody will poll again.
        // The work it started is lost when fut drops.
    }
}
```

This fails because there is no loop, no task table, and no wake path. When `poll` returns `Pending`, the future is dropped and every step of work it completed is discarded. A single call to `poll` is not enough; the future needs a mechanism that stores it, waits for a wake, and polls it again.

### Why Any Runtime Needs This

Every async runtime must solve the same problem: a future that returns `Pending` cannot run itself, so something else must store it and re-poll it. Tokio, async-std, and smol all implement an event loop that drains a ready queue, parks the thread when idle, and wakes on I/O or timer events.

`Mar` is a single-threaded executor. One thread, one ready queue, one reactor, one timer registry. The simplicity is the point: no locks on user data, no data races in user code, deterministic task ordering. It suffices when the total work fits on one core; CPU-bound work is offloaded to a worker pool.

The contract: `Mar::run(future)` drives `future` — and anything it spawns or wakes — to completion, then returns `io::Result<()>`. The future must be `Future<Output = ()> + 'static`. The `'static` bound means it must own everything it needs.

## 2. How This Runtime Implements Mar

### The Struct

`src/mar.rs`:

```rust
pub struct Mar {
    pub(crate) state: Rc<RefCell<RuntimeState>>,
    pub(crate) wheel: Rc<TimerRegistry>,
    pub(crate) reactor: ReactorHandle,
    pub(crate) pool: WorkerPool,
    events: mio::Events,
}
```

### Field by Field

**`state: Rc<RefCell<RuntimeState>>`.** The shared scheduling state — ready queue, task table, id counter, blocking-waker map. `Rc` gives shared ownership across the executor, the `ContextHandle`, and each `TaskWaker`. `RefCell` gives interior mutability on a single thread. `Rc<RefCell<...>>` is not `Send`, which is correct because the state is never shared with the worker pool.

**`wheel: Rc<TimerRegistry>`.** The timer registry, an unsorted list of `(deadline, id, Waker)` entries. `sleep()` pushes entries on first `poll`; the executor reads it to compute the park timeout and fire expired timers. Shared with the `ContextHandle` so `sleep()` can reach it.

**`reactor: ReactorHandle`.** `Rc<RefCell<Reactor>>`. The `Reactor` wraps `mio::Poll`, the OS-level I/O multiplexer (`epoll` on Linux, `kqueue` on macOS) that blocks the thread until any registered file descriptor becomes ready or a timeout elapses.

**`pool: WorkerPool`.** Three OS threads by default that run blocking closures off the executor thread. Workers receive closures via a job channel, run them inside `catch_unwind`, and report completion by sending a `BlockingId` through a completed channel and calling `wake()` on a `mio::Waker` registered with the reactor.

**`events: mio::Events`.** The buffer `mio::Poll::poll` fills with I/O events. Allocated once with capacity 64 and reused every iteration to avoid per-cycle heap allocation.

### Construction

`Mar::new` in `src/mar.rs`:

```rust
pub(crate) fn new() -> Self {
    let state = RuntimeState::new();
    let wheel = Rc::new(TimerRegistry::new());
    let reactor = Rc::new(RefCell::new(Reactor::new()));
    let pool = {
        let reactor = reactor.borrow();
        let registry = reactor.registry();
        let waker = mio::Waker::new(registry, WAKEN_TOKEN);
        let waker = Arc::new(waker.unwrap());
        WorkerPool::new(waker)
    };
    let events = mio::Events::with_capacity(64);
    Mar { state, wheel, reactor, pool, events }
}
```

The reactor is created first because the pool's `mio::Waker` must be registered with the *same* `mio::Poll` the reactor wraps — that is the only way a worker's `wake()` can unblock the executor's park. `WAKEN_TOKEN` is `mio::Token(0)`, reserved so the pool's wake is distinguishable from future I/O events.

### `run()`, Line by Line

```rust
pub fn run<F>(future: F) -> io::Result<()>
where
    F: Future<Output = ()> + 'static,
{
    let mut runtime = Self::new();

    let _context = context::install(context::ContextHandle {
        state: runtime.state.clone(),
        wheel: runtime.wheel.clone(),
        job_tx: runtime.pool.job_tx(),
        completed_tx: runtime.pool.completed_tx(),
    });

    spawn_root(&runtime, future);

    loop {
        drain_ready_queue(&runtime);

        if is_done(&runtime) {
            return Ok(());
        }

        poll_readiness_events(&mut runtime)?;
        fire_due_timers(&runtime);
        wake_completed_blocking(&runtime);
    }
}
```

**Step 1: `Self::new()`.** Build the runtime.

**Step 2: `context::install(...)`.** Install a `ContextHandle` into a `thread_local!` slot so any code on this thread can reach the runtime's services via `context::with(|ctx| ...)`. The return value is a `ContextGuard` — a RAII struct whose only job is to call `uninstall()` when it drops. The guard is bound to `_context`, declared *after* `runtime`. Locals drop in reverse declaration order, so `_context` drops *before* `runtime`. This is load-bearing: the guard releases the thread-local's clones of the pool's senders before the pool's `Drop` closes the job channel and joins its workers. If the context were still installed during the pool's drop, the live sender clone would keep the channel open and `join()` would block forever.

**Step 3: `spawn_root(&runtime, future)`:**

```rust
fn spawn_root<F>(runtime: &Mar, future: F)
where
    F: Future<Output = ()> + 'static,
{
    let mut state = runtime.state.borrow_mut();
    let id = state.next_id;
    state.next_id.0 += 1;
    let waker = TaskWaker::new(state.queue.clone(), id);
    state.tasks.insert(id, Task::new(id, future, waker));
    state.queue.lock().unwrap().push(id);
}
```

Read the current id, increment the counter (ids are never reused), build the waker from the shared ready queue and the id, insert the task into the table, push the id onto the queue. The future has entered the runtime.

**Step 4: The loop.** Four phases.

**`drain_ready_queue`:**

```rust
fn drain_ready_queue(runtime: &Mar) {
    loop {
        let next = {
            let state = runtime.state.borrow_mut();
            let mut queue = state.queue.lock().unwrap();
            if queue.is_empty() { None } else { Some(queue.remove(0)) }
        };
        let Some(id) = next else { break; };

        let Some(mut task) = runtime.state.borrow_mut().tasks.remove(&id) else {
            continue;
        };

        let waker = task.waker().clone();
        let mut cx = Context::from_waker(&waker);
        match task.poll(&mut cx) {
            Poll::Pending => {
                runtime.state.borrow_mut().tasks.insert(id, task);
            }
            Poll::Ready(()) => {}
        }
    }
}
```

Pops ids from the queue one at a time, looks up each task, polls it. The scoped-borrow pattern — `let next = { ... }` — ensures the `RefCell` borrow ends before `task.poll` runs, because a poll executes arbitrary user code that might `borrow_mut` the state again. `queue.remove(0)` is FIFO. The `else { continue; }` skips stale ids whose tasks are already gone — spurious wakes are harmless.

The task is removed from the table *before* polling. If `poll` returns `Ready(())`, the task is dropped and never re-inserted. If `Pending`, the task is re-inserted — *parked*: present in the table, absent from the queue, waiting for its waker.

**`is_done`:**

```rust
fn is_done(runtime: &Mar) -> bool {
    let state = runtime.state.borrow();
    state.tasks.is_empty() && runtime.wheel.is_empty() && state.blocking_wakers.is_empty()
}
```

Three conditions, all of which must hold. The ready queue is not checked — it was already drained. These three check for work that will arrive *later*: a parked task will be woken, a timer will expire, a blocking job will finish.

**`poll_readiness_events`:**

```rust
fn compute_timeout(wheel: &TimerRegistry) -> Option<Duration> {
    wheel.next_deadline()
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
}
```

`next_deadline()` returns the earliest `Instant` among registered timers. `saturating_duration_since(Instant::now())` computes the remaining time — `Duration::ZERO` if overdue (park returns immediately), `None` if no timers (park forever). `reactor.borrow_mut().poll(&mut events, timeout)` blocks the thread *inside the kernel* until an event arrives or the timeout expires.

**`fire_due_timers`:** `wheel.expire_due()` scans all entries, collects the wakers of expired entries, removes them, then calls `wake_by_ref()` on each. The borrow is released before waking — a waker's code might re-enter the registry.

**`wake_completed_blocking`:** For each event with `WAKEN_TOKEN`, drains completed `BlockingId`s from the pool's channel, looks up each in `blocking_wakers`, and calls `wake_by_ref()` to push the task's id onto the ready queue.

### Shutdown

When `is_done` returns `true`, `run()` returns. `_context` drops first, releasing the thread-local sender clones. Then `runtime` drops. `WorkerPool::drop` calls `shutdown()`:

```rust
pub(crate) fn shutdown(&mut self) {
    drop(self.job_tx.take());
    for handle in self.workers.drain(..) {
        let _ = handle.join();
    }
}
```

`job_tx.take()` moves the sender out of the `Option` and drops it, closing the channel. Workers' `recv()` returns `Err`, they exit, and `join()` completes. The panic path is the same: the unwind drops the guard, which uninstalls the context, and the pool joins.

### Worked Example

Trace `Mar::run(async { time::sleep(Duration::from_secs(1)).await })`:

1. **Construction.** `TaskId(0)` assigned, task inserted, id pushed. State: queue `[TaskId(0)]`, tasks `{TaskId(0)}`.
2. **First drain.** Pop `TaskId(0)`, poll. The root future calls `sleep(1s)`, which returns a `Sleep` future. `Sleep::poll`: deadline not reached, calls `registry.push(deadline, waker)` (timer id `0`), returns `Pending`. Task re-inserted. State: queue `[]`, tasks `{TaskId(0)}`, timer registry has entry at `t₀ + 1s`.
3. **`is_done`.** Task table non-empty. Returns `false`.
4. **Park.** `compute_timeout` returns `Some(1s)`. `reactor.poll` blocks the thread in the kernel for ~1 second.
5. **Timer fires.** `expire_due()` collects the expired entry's waker, calls `wake_by_ref()`, pushing `TaskId(0)` to the queue.
6. **Second drain.** Pop `TaskId(0)`, poll. `Sleep::poll`: `Instant::now() >= deadline`, returns `Ready(())`. Root future completes, task dropped. State: queue `[]`, tasks `{}`.
7. **`is_done`.** All three empty. `run()` returns `Ok(())`.

The executor looped twice: once to park and wait, once to drain after the timer fired.

### Source Links

- `src/mar.rs` — `Mar`, `new`, `run`, `spawn_root`, `drain_ready_queue`, `is_done`, `poll_readiness_events`, `compute_timeout`, `fire_due_timers`, `wake_completed_blocking`
- `src/context.rs` — `ContextHandle`, `install`, `uninstall`, `with`, `ContextGuard`
- `src/runtime_state.rs` — `RuntimeState`, `TaskId`, `BlockingId`
- `src/task/worker_pool.rs` — `WorkerPool`, `Job`, `shutdown`
- `src/reactor.rs` — `Reactor`, `ReactorHandle`

## 3. Design Decisions and Tradeoffs

**Why `Rc<RefCell<RuntimeState>>` and not `Arc<Mutex<...>>`.** Single-threaded invariant. `Rc` gives cheap shared ownership with no atomic operations; `RefCell` gives runtime-checked interior mutability that panics loudly on double-borrow, whereas a `Mutex` would hide ordering bugs as deadlocks. The one field that *must* be `Arc<Mutex<...>>` is the ready queue, because `Waker::from(Arc<T>)` requires `T: Send + Sync`, and `Rc` is not `Send`. This would change for a multi-threaded executor.

**Why drain to exhaustion before checking I/O.** `drain_ready_queue` runs every ready task before the executor parks. This prevents starvation — a task that spawns another task, which spawns another, all run in the same drain cycle — and keeps the invariant simple: at the end of the drain, the queue is provably empty. The alternative (bounded drain per iteration) would give interleaving fairness but complicate reasoning for no single-threaded benefit.

**Why `saturating_duration_since`.** If a deadline passed during the drain, the duration is `Duration::ZERO` — the park returns immediately and overdue timers fire in the same iteration. Without it, an overdue timer would underflow the duration calculation.

**What if a task never yields.** A task that never returns `Pending` starves the runtime. The executor cannot preempt it — there is no timer interrupt, no way to interrupt a `poll` call. This is cooperative scheduling's fundamental constraint. The `Probe` test future demonstrates the normal pattern: return `Pending`, re-wake, let other tasks run.

## 4. Failure Modes and Misconceptions

### What Breaks If Implemented Wrong

**Hold a `RefCell` borrow across `poll`.** Any future that calls `sleep` or `spawn_root` during its poll would panic with a double-borrow. The scoped-borrow pattern in `drain_ready_queue` is the defense.

**Declare the context guard before the runtime.** `runtime` would drop first, `WorkerPool::drop` would call `join()`, but the still-installed context holds a live `job_tx` clone, so the channel never closes and `join()` blocks forever. The tests `runtime_drop_joins_workers_promptly_after_run` and `panicking_root_future_still_joins_workers` assert `run()` returns promptly — that assertion *is* the deadlock check.

**Wake wakers while holding the timer registry borrow.** A waker whose code re-entered the registry would panic on double-borrow. `expire_due`'s collect-then-wake pattern prevents this.

### Common Misunderstandings

**"The executor polls tasks in a loop forever."** It parks when idle. The thread blocks inside `mio::Poll::poll`, consuming no CPU, until an event or wake unblocks it.

**"`run` is just `future.await`."** It is a full event loop: build a runtime, install a context, spawn the root future, drain-park-wake until all work is finished.

**"`is_done` checks the ready queue."** The queue was already drained. `is_done` checks the task table, timer registry, and blocking-waker map — structures that hold work arriving *later*.

**"A panic in a task cancels the runtime."** The unwind drops the guard, uninstalls the context, and joins the pool. The panic propagates to the caller. No workers are stranded.

## 5. Summary

- `Mar` is the single-threaded executor: it owns the ready queue, task table, timer registry, reactor, and worker pool, and runs an event loop that drains ready tasks, parks when idle, and wakes on I/O, timers, or blocking completions.
- `run()` installs a thread-local context, spawns the root future, and loops through `drain_ready_queue` → `is_done` → `poll_readiness_events` → `fire_due_timers` → `wake_completed_blocking` until all work is finished.
- It depends on `RuntimeState` for scheduling state, `TimerRegistry` for deadlines, `Reactor` for the OS poller, `WorkerPool` for blocking work, and `ContextHandle` so leaf futures can reach these services.
- The context guard's drop order — before the runtime — is the load-bearing detail that prevents the worker pool from deadlocking on shutdown.
