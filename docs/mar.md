# Mar: The Heartbeat of the Runtime

## A Gentle Start

An `async fn` has been written. The compiler turned it into a state machine. The standard library provides `Future`, `Pin`, and `Waker`. But none of those things actually *run* the code. A future is like a recipe — it describes what to do, but someone has to walk into the kitchen and start cooking. That someone is the **executor**.

Mar is this runtime's executor. It's a single-threaded event loop: one thread, one queue of work, one place where every task takes its turn. It waits until something interesting happens, then runs whatever became ready. When nothing is ready, it parks the thread — not busy-waiting, not sleeping on a timer, but truly parked inside the operating system, consuming no CPU until the kernel wakes it up.

For anyone who has ever wondered what happens between writing `.await` and the moment code resumes, Mar is the answer.

## Why a Future Cannot Be Polled Just Once

When first encountering Rust's `Future` trait, the temptation is to call `poll` manually and be done with it. Build a `Context`, call `poll`, check the result. If it's `Ready`, the future produced a value. If it's `Pending`, the operation is stuck. The future said "not yet, try again later," but there is no mechanism for "later." `poll` was called once, nobody is holding the waker, and nobody will ever come back to try again.

A single call to `poll` is like knocking on a door once and walking away before anyone answers — the caller will never know if someone was coming. The future might have been waiting for a timer to expire or a worker thread to finish a job, but without something to receive the wake signal and re-queue the poll, the work it represents simply evaporates. The state machine that the compiler built — the variables held across `.await` points, the progress through the async block — all of it drops silently, and nothing written after that `.await` ever runs.

An executor fixes every part of this. It stores the future so it doesn't disappear. It loops, polling every task that's ready, and when nothing is ready it parks the thread efficiently instead of spinning. When something outside the executor — an I/O event, a timer firing, a blocking job finishing — signals that work is ready, the executor picks up the signal, re-queues the waiting task, and polls it again. This cycle continues until every task has finished and there is genuinely nothing left to do. The `async` code never leaves the executor's care; Mar owns it, stores it, polls it, and re-polls it, for as long as it takes.

## The Contract

`Mar::run(future)` takes one future and drives it to completion. It also drives anything that future spawns, anything those spawn, and anything woken by timers or blocking work. It returns `io::Result<()>` — either everything finished cleanly, or the operating system said "something went wrong."

The future must be `Future<Output = ()> + 'static`. The `()` output means the executor doesn't return a value — code communicates results through channels and shared state. The `'static` bound means the future must own everything it needs; it can't borrow from a parent scope that might disappear while the runtime is still running.

## The Four Fields of Mar

```rust
pub struct Mar {
    pub(crate) state: Rc<RefCell<RuntimeState>>,
    pub(crate) wheel: Rc<TimerRegistry>,
    pub(crate) reactor: ReactorHandle,
    pub(crate) pool: WorkerPool,
    events: mio::Events,
}
```

Mar owns four things, and each one solves a different problem:

**`state` — the scheduling brain.** This is the runtime's shared memory. It holds the ready queue (which tasks need polling now?), the task table (which tasks exist at all?), and the bookkeeping for blocking work. Every part of the runtime that schedules — spawning, waking, completing — reads and writes this one struct. `Rc` means multiple handles share one allocation; `RefCell` means it can be mutated through shared references, with the borrow checker watching at runtime instead of compile time.

**`wheel` — the timer registry.** When `time::sleep(duration)` is called, the resulting `Sleep` future registers itself here as a `(deadline, waker)` pair. The executor reads this registry every iteration to figure out how long to park. No dedicated timer thread needed — the OS's own timeout mechanism does the waiting.

**`reactor` — the OS poller.** This wraps `mio::Poll`, which wraps `epoll` on Linux (or `kqueue` on macOS). It's the mechanism that parks the thread inside the kernel and wakes it when *any* watched file descriptor becomes ready. Right now the reactor watches exactly one thing: the worker pool's wake signal. But the door is open for socket I/O later.

**`pool` — the worker threads.** Three OS threads by default. They run blocking closures so the executor thread never freezes. When `task::spawn_blocking(|| compute_expensive_thing())` is called, the closure gets sent to one of these threads through a channel. The result comes back through another channel, and the pool wakes the executor.

**`events` — a reusable buffer.** `mio::Events::with_capacity(64)` allocates space for up to 64 OS-level events once, at construction, and reuses that buffer every iteration. No per-loop allocations.

## The Event Loop, Step by Step

Here is the entire loop:

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

Let's walk through each part.

### Step 1: Build the Runtime

`Self::new()` creates Mar. It builds the state, the timer registry, the reactor, and the worker pool — all wired together. The reactor comes first because the pool's `mio::Waker` must be registered with the *same* `mio::Poll` that Mar will later call `poll` on. If the waker talked to a different poller, calling `wake()` would signal nobody, and the executor would park forever.

### Step 2: Install the Thread-Local Context

This is a crucial detail that's easy to overlook. `context::install` writes a `ContextHandle` into a per-thread slot — a `thread_local!` variable. Any code on this thread can now call `context::with(|ctx| ...)` to reach the timer registry, the scheduling state, or the worker pool channels. This is how `time::sleep` and `task::spawn_blocking` work without requiring a handle to be passed around.

The return value is a `ContextGuard`. It holds no data — its only job is to run `uninstall()` when it drops. And it's declared *after* `runtime`, so it drops *before* `runtime` (Rust drops locals in reverse declaration order). This ordering is load-bearing: the guard releases its clones of the pool's channel senders before the pool shuts down. If the context were still alive during shutdown, the channel wouldn't close, workers would block forever, and `run()` would never return.

### Step 3: Spawn the Root Future

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

The future gets an id, a waker, and a place in the task table. Then its id gets pushed onto the ready queue. It's now in the system.

### Step 4: The Loop — Drain, Check, Park, Wake

The loop has four phases per iteration:

**Phase A: `drain_ready_queue`** — Poll everything that's ready right now. It pops task ids from the ready queue one at a time, looks up each task in the table, and polls it. If the task returns `Ready(())`, it's dropped and gone. If it returns `Pending`, it's re-inserted into the table — *parked*: alive, but not scheduled, waiting for its waker to fire.

The drain runs to exhaustion every iteration. This means a task that spawns another task, which spawns another — they all run in the same drain cycle. No task starves just because it was spawned later in the same batch.

**Phase B: `is_done`** — Are we finished?

```rust
fn is_done(runtime: &Mar) -> bool {
    let state = runtime.state.borrow();
    state.tasks.is_empty()
        && runtime.wheel.is_empty()
        && state.blocking_wakers.is_empty()
}
```

Three conditions, all of which must hold. The ready queue isn't checked because it was just drained. These three check for work that *will arrive later*: a parked task waiting for its waker, a timer that hasn't fired yet, a blocking job still running on a worker thread.

**Phase C: `poll_readiness_events`** — Park the thread.

```rust
fn compute_timeout(wheel: &TimerRegistry) -> Option<Duration> {
    wheel.next_deadline()
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
}
```

This computes how long to park. If there's a timer with 800ms until its deadline, the timeout is `Some(800ms)`. If no timers exist, it's `None` — park forever until an I/O event arrives. `saturating_duration_since` means if a deadline has already passed while the executor was draining, the result is `Duration::ZERO` and the poll returns immediately.

Then `reactor.poll(&mut events, timeout)` blocks the thread inside a kernel call (`epoll_wait` on Linux). The thread is suspended — no Rust code is running, no CPU cycles are consumed. The kernel wakes it when either a registered file descriptor becomes ready or the timeout expires.

**Phase D: Handle wakeups.** Two things might have happened:

- `fire_due_timers` scans the timer registry for expired entries, collects their wakers, and calls `wake_by_ref()` to push the corresponding task ids onto the ready queue.
- `wake_completed_blocking` checks if the worker pool woke the executor. If it did, it drains the completed-job channel, finds the task waiting on each completed job, and wakes it.

Then the loop repeats. Each iteration, the ready queue gets drained again, and any newly-woken tasks get their turn.

### The Shutdown Dance

When `is_done` returns `true`, `run()` exits. Then Rust drops the locals:

1. `_context` drops first → `ContextGuard::drop` calls `uninstall()` → the thread-local goes back to `None`. The clones of the pool's channel senders inside the context handle are released.
2. `runtime` drops next → `WorkerPool::drop` calls `shutdown()`, which drops the job channel sender. Every worker's `recv()` returns `Err` (channel closed). Each worker exits its loop, and `join()` completes. Clean shutdown, no deadlocks.

If a task panics instead of returning normally, the same sequence happens — the panic unwinds through `run()`, `_context` drops, `runtime` drops, workers join. No threads are left stranded.

## Walking Through a Sleep

Let's trace `Mar::run(async { time::sleep(Duration::from_secs(1)).await })`:

1. **Construction.** The root task gets `TaskId(0)`, is inserted into the table, and its id is pushed onto the queue.
2. **First drain.** The task is polled. It calls `sleep(1s)`, which creates a `Sleep` future. `Sleep::poll` checks: has 1 second passed? No. It registers `(deadline, waker)` in the timer registry and returns `Pending`. The task goes back into the table, parked.
3. **`is_done`.** The table isn't empty — there's a parked task. Not done.
4. **Park.** `compute_timeout` sees the timer registry has one entry with deadline ~1 second from now. The reactor parks the thread for 1 second inside the kernel.
5. **Timer fires.** `fire_due_timers` scans the registry. The deadline has passed. It collects the waker, calls `wake_by_ref()`, and `TaskId(0)` lands on the ready queue. The timer entry is removed.
6. **Second drain.** The task is polled again. `Sleep::poll` sees `Instant::now() >= deadline` and returns `Ready(())`. The root future completes. The task drops.
7. **`is_done`.** Table empty, registry empty, blocking wakers empty. `run()` returns `Ok(())`.

Two iterations of the loop. Two polls. One real wait inside the kernel.

## What Mar Provides (And What It Doesn't)

Mar is deliberately simple. One thread. One ready queue. No locks on data. No data races in code. Tasks run in the order they were woken — deterministic and predictable.

But it's cooperative. A task that never returns `Pending` runs forever, and nobody can stop it. There's no preemption, no timer interrupt that yanks control away. If a task loops without yielding, every other task freezes. This is the fundamental tradeoff of async Rust: performance and simplicity, at the cost of requiring the programmer to decide when to yield.

## Design Choices Worth Noticing

**Why `Rc<RefCell<RuntimeState>>` instead of `Arc<Mutex<...>>`?** Because Mar is single-threaded. `Rc` is cheaper than `Arc` (no atomic operations). `RefCell` panics loudly on a double-borrow, whereas a `Mutex` would deadlock silently — debugging a deadlock is much harder than reading a panic message with a line number.

**Why drain to exhaustion?** It's the simplest invariant: after the drain, the queue is provably empty. It prevents a subtle class of starvation bugs. And on a single thread, there's no fairness argument for bounding the drain — nothing else needs the CPU.

**Why `saturating_duration_since`?** If a deadline passes while the executor is busy draining tasks, the duration goes to zero, not negative. The reactor returns immediately, and the overdue timer fires in the same iteration.

## Summary

Mar is the loop that gives futures a pulse. It builds the runtime, installs a thread-local context, spawns the future, and enters a four-phase cycle: drain ready tasks, check if done, park the thread until something happens, and handle whatever woke the executor. It works until every task, timer, and blocking job is resolved.

Mar depends on `RuntimeState` to track what's running and what's waiting, `TimerRegistry` to manage deadlines, `Reactor` to interact with the OS, `WorkerPool` to run blocking work, and `ContextHandle` to let code reach all of these without threading handles through every function signature.

Source: `src/mar.rs`
