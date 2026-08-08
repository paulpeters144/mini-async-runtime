# ContextHandle: How Futures Reach Runtime Services

## 1. Concept: How Do Futures Reach Runtime Services?

### The Core Concept

While `Mar::run` executes, a per-thread slot holds handles to the runtime's internal services — the scheduling state, the timer registry, and the worker-pool channels. Any future code running on that thread reads those handles from the slot without receiving them as an argument. This per-thread slot is the `ContextHandle`.

### The Problem

A deeply nested `async fn` needs to call `time::sleep(Duration::from_secs(1))` or `task::spawn_blocking(|| expensive_work())`. Both of these functions need runtime internals: `sleep` needs the timer registry (a structure that tracks deadlines), and `spawn_blocking` needs a channel to send closures to the worker pool and a channel to receive completion notifications. The runtime owns these structures. The future needs them. The question is how the future gets them.

The naive approach is to pass a handle explicitly:

```rust
async fn my_work(handle: &RuntimeHandle) {
    let result = handle.spawn_blocking(|| compute()).await;
    handle.sleep(Duration::from_secs(1)).await;
    // ... more work ...
}
```

This fails for three reasons. First, every `async fn` in the program would need a `handle` parameter. Every caller would need to forward it. The signature of every function changes, and the parameter infects the entire call tree. Second, async functions are compiler-generated state machines. When `my_work` reaches an `.await`, it returns `Pending` to the executor. When the executor re-polls it later, it must supply the same handle again. The handle must be stored inside the state machine, which means it must be `'static` (it outlives any single call frame), but a borrowed reference `&RuntimeHandle` has a limited lifetime. Third, the Rust ecosystem does not expect a runtime parameter. Every async library — `tokio`, `smol`, `async-std` — provides functions like `sleep` and `spawn` as plain free functions. If `mar` required a handle argument, its futures would not be interchangeable with ecosystem futures.

### Why Any Runtime Needs This

Every async runtime must solve the same problem: deeply nested code must reach the runtime's services without explicit parameter passing. Tokio uses a thread-local `CURRENT` runtime handle. Smol uses a thread-local executor reference. The mechanism is the same: store the handle where any code on the thread can find it, and remove it when the runtime shuts down. `mar` uses `thread_local!` to implement this, which is the standard Rust primitive for per-thread storage.

---

## 2. How This Runtime Implements ContextHandle

### The Struct

From `src/context.rs`:

```rust
pub(crate) struct ContextHandle {
    pub state: Rc<RefCell<RuntimeState>>,
    pub wheel: Rc<TimerRegistry>,
    pub job_tx: mpsc::Sender<Job>,
    pub completed_tx: mpsc::Sender<BlockingId>,
}
```

### Field by Field

- **`state: Rc<RefCell<RuntimeState>>`** — a reference-counted, interior-mutable handle to the runtime's scheduling state. `Rc` (reference-counted pointer) gives shared ownership: multiple parts of the code can hold a handle to the same `RuntimeState` without duplicating it. `RefCell` provides runtime-checked interior mutability: the contents can be borrowed mutably through a shared reference, and the borrow checker defers the check to runtime (a double-borrow panics instead of compiling). `spawn_blocking` reads `state` to allocate a `BlockingId` from `next_blocking_id` and to register a waker in `blocking_wakers`. The executor (`Mar`) also holds an `Rc<RefCell<RuntimeState>>` — both point to the same allocation.

- **`wheel: Rc<TimerRegistry>`** — a reference-counted handle to the timer registry, which stores `(deadline, waker)` entries for active `Sleep` futures. `time::sleep` clones this handle to build a `Sleep` future that can register itself. The executor also holds an `Rc<TimerRegistry>` — both point to the same structure.

- **`job_tx: mpsc::Sender<Job>`** — the sending end of a multi-producer, single-consumer (`mpsc`) channel. `mpsc::Sender` is the standard library's channel type: `send` pushes a value into the channel, and the receiver (`mpsc::Receiver`) pulls values out on the other end. `Job` is a type alias for `Box<dyn FnOnce() + Send + 'static>` — a boxed closure that can be sent to another thread and run once. `spawn_blocking` clones this sender to dispatch closures to the worker pool's receiver.

- **`completed_tx: mpsc::Sender<BlockingId>`** — the sending end of a second channel. This one carries `BlockingId` values (a newtype over `usize` that identifies a specific blocking job) from worker threads back to the executor. When a worker finishes a closure, it sends the `BlockingId` through this channel so the executor knows which task to wake.

Each field is a clone of a handle that the executor (`Mar`) owns. The `ContextHandle` does not own any of these structures; it holds shared references to them.

### The Thread-Local Slot

```rust
thread_local! {
    static CONTEXT: RefCell<Option<ContextHandle>> = const { RefCell::new(None) };
}
```

The `thread_local!` macro declares a static variable whose value is per-thread. Each thread gets its own copy, initialized lazily on first access. The `const { ... }` block ensures the initialization expression is evaluated at compile time, so there is no runtime cost on first access beyond the thread-local lookup.

The type is `RefCell<Option<ContextHandle>>`. `RefCell` provides interior mutability: the slot must be writable (to install and uninstall the handle) and readable (to look it up). `Option` represents the two states: `Some(handle)` when `Mar::run` is executing on this thread, `None` otherwise. A `RefCell<Option<...>>` is the standard pattern for a mutable thread-local that may or may not hold a value.

### Install and Uninstall

```rust
pub(crate) fn install(handle: ContextHandle) -> ContextGuard {
    CONTEXT.with(|c| *c.borrow_mut() = Some(handle));
    ContextGuard
}
```

`install` writes a `ContextHandle` into the thread-local slot. `CONTEXT.with(|c| ...)` accesses the per-thread `RefCell`. `c.borrow_mut()` takes a mutable borrow of the `Option`. `*c.borrow_mut() = Some(handle)` overwrites the `Option` with the new handle. If a previous handle was installed (from a nested `run()`), it is dropped here.

The function returns a `ContextGuard`:

```rust
pub(crate) struct ContextGuard;

impl Drop for ContextGuard {
    fn drop(&mut self) {
        uninstall();
    }
}
```

`ContextGuard` is an RAII guard. RAII (Resource Acquisition Is Initialization) means the struct's lifetime controls a resource — here, the thread-local's contents. `ContextGuard` holds no data. Its only purpose is to run `uninstall()` when it drops. Rust guarantees that `Drop::drop` runs when a value goes out of scope, whether that happens through normal return or during a panic unwind. This means the thread-local is always cleared, no matter how `Mar::run` exits.

```rust
pub(crate) fn uninstall() {
    CONTEXT.with(|c| *c.borrow_mut() = None);
}
```

`uninstall` sets the slot back to `None`, releasing the `ContextHandle` and all the handles it holds. After `uninstall`, any call to `with` on this thread will panic — there is no runtime installed.

### The `with` Function

```rust
pub(crate) fn with<F, R>(f: F) -> R
where
    F: FnOnce(&ContextHandle) -> R,
{
    CONTEXT.with(|c| f(c.borrow().as_ref().expect("runtime context not installed")))
}
```

`with` reads the thread-local and calls `f` with a reference to the installed `ContextHandle`. `c.borrow()` takes a shared borrow of the `Option`. `.as_ref()` converts `&Option<ContextHandle>` to `Option<&ContextHandle>`. `.expect(...)` unwraps the `Option`, panicking with the message `"runtime context not installed"` if it is `None`.

The panic is deliberate. Calling `time::sleep` or `task::spawn_blocking` outside `Mar::run` is a programming error. There is no meaningful default behavior — a `sleep` with no timer registry would produce a future that never resolves. A panic with a clear message is the correct response to a programming error.

### The Drop-Order Subtlety in `Mar::run`

This is the load-bearing detail of the entire design. From `src/mar.rs`:

```rust
let mut runtime = Self::new();

let _context = context::install(context::ContextHandle {
    state: runtime.state.clone(),
    wheel: runtime.wheel.clone(),
    job_tx: runtime.pool.job_tx(),
    completed_tx: runtime.pool.completed_tx(),
});
```

Locals drop in reverse declaration order. `runtime` is declared first, `_context` second. When `run` returns (or panics), `_context` drops first, then `runtime`. This order is critical.

When `_context` drops, `ContextGuard::drop` calls `uninstall()`, which sets the thread-local to `None`. The `ContextHandle` that was stored there held clones of `runtime.pool.job_tx()` and `runtime.pool.completed_tx()` — senders for the worker pool's channels. Those clones are dropped now.

When `runtime` drops next, `Mar`'s fields drop. `WorkerPool::drop` calls `shutdown()`, which does `drop(self.job_tx.take())` — taking the original sender out and dropping it, closing the job channel. Then it joins all worker threads. Each worker's `recv()` returns `Err` (channel closed), and the worker exits.

If the `_context` guard did not exist — or if it were declared before `runtime` — the drop order would reverse. `runtime` would drop first. `WorkerPool::shutdown` would try to close the job channel, but the `ContextHandle` still held a clone of `job_tx`. The channel would not close because there is still a live sender. Every worker's `recv()` would block forever, waiting for a job that never comes. `join()` would block forever. `run()` would never return.

The guard ensures the thread-local's sender clones are released before the pool shuts down, so the channel closes cleanly and workers join promptly.

### Worked Example: `spawn_blocking` Inside `run`

Trace `Mar::run(async { task::spawn_blocking(|| 42).await })` from the call site of `spawn_blocking`:

1. `spawn_blocking` (`src/task/blocking.rs`) calls `context::with(|ctx| ...)`. This reads the thread-local slot, finds `Some(handle)`, and clones three fields: `ctx.state` (an `Rc<RefCell<RuntimeState>>`), `ctx.job_tx` (an `mpsc::Sender<Job>`), and `ctx.completed_tx` (an `mpsc::Sender<BlockingId>`).

2. `spawn_blocking` borrows `state` mutably, reads `next_blocking_id` (say `BlockingId(0)`), increments it to `BlockingId(1)`, and releases the borrow.

3. `spawn_blocking` creates a per-job result channel: `mpsc::channel::<std::thread::Result<R>>()`. This is a private channel between the worker and this specific `BlockingTask`.

4. `spawn_blocking` builds the job closure:
   ```rust
   let job: Job = Box::new(move || {
       let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| 42));
       let _ = tx.send(Ok(42));
       let _ = completed_tx.send(BlockingId(0));
   });
   ```
   The closure owns the result sender (`tx`) and a clone of `completed_tx`. When it runs on a worker thread, it executes `|| 42`, catches any panic, sends the result through the private channel, and sends `BlockingId(0)` through the completed channel.

5. `spawn_blocking` calls `job_tx.send(job)` — the closure is pushed into the job channel.

6. `spawn_blocking` returns `BlockingTask { state, id: BlockingId(0), rx: Some(rx), done: false }`.

7. The executor polls this `BlockingTask`. On the first poll, the task registers its waker in `state.blocking_wakers` under `BlockingId(0)`. It then calls `rx.try_recv()`, which returns `Err(Empty)` — the worker has not finished yet. The task returns `Pending`.

8. A worker thread calls `rx.lock().unwrap().recv()`, receives the closure, runs it inside `catch_unwind`, sends `Ok(42)` on the result channel, and sends `BlockingId(0)` on the completed channel. The worker then calls `w.wake()` on its `mio::Waker`, which unblocks the executor's park.

9. The executor's `poll_readiness_events` returns. `wake_completed_blocking` sees `WAKEN_TOKEN`, calls `pool.drain_completed()` to get `[BlockingId(0)]`, looks up the waker in `state.blocking_wakers`, and calls `wake_by_ref()` on it. This pushes the task's id into the ready queue.

10. The next `drain_ready_queue` pops the task's id, looks up the `BlockingTask` in the task table, and re-polls it. This time `rx.try_recv()` returns `Ok(Ok(42))`. The task removes its entry from `blocking_wakers` and returns `Ready(42)`.

### Worked Example: `time::sleep` Inside `run`

Trace `Mar::run(async { time::sleep(Duration::from_secs(1)).await })`:

1. `time::sleep` (`src/time/sleep.rs`) calls `context::with(|ctx| ctx.wheel.clone())`, cloning the `Rc<TimerRegistry>`.

2. `sleep` creates `Sleep { registry, deadline: Instant::now() + Duration::from_secs(1), id: None, done: false }`.

3. The executor polls this `Sleep`. On the first poll, `Instant::now() < deadline`, so it calls `registry.push(deadline, cx.waker().clone())`, which returns id `0`. The task returns `Pending`.

4. The executor drains the queue (empty), checks `is_done` (false — the timer registry is non-empty), and calls `compute_timeout`, which calls `wheel.next_deadline()` to get `Some(deadline)`. The timeout is `deadline.saturating_duration_since(Instant::now())`, which is approximately 1 second.

5. The executor calls `reactor.poll(&mut events, Some(1s))`. The thread blocks inside the kernel for ~1 second.

6. The timeout expires. `poll` returns with an empty `events` buffer.

7. `fire_due_timers` calls `wheel.expire_due()`. The entry for `id: 0` has `deadline <= now`, so its waker is collected and `wake_by_ref()` is called. The task's id is pushed into the ready queue.

8. The next `drain_ready_queue` re-polls the `Sleep`. `Instant::now() >= deadline` is true, so `done` is set to `true` and `Ready(())` is returned.

### Worked Example: Calling `sleep` Outside `run`

If a user writes:

```rust
fn main() {
    let _ = time::sleep(Duration::from_secs(1));
}
```

`time::sleep` calls `context::with(|ctx| ctx.wheel.clone())`. The thread-local slot is `None` (no `Mar::run` is executing). `.expect("runtime context not installed")` panics with that message. This is the designed behavior: there is no timer registry to register with, and a silent no-op would produce a future that never resolves.

### Interactions

- **Installed by:** `Mar::run` (`src/mar.rs`), which calls `context::install(...)` after building the runtime.
- **Read by:** `time::sleep` (`src/time/sleep.rs`), which clones `wheel`. `task::spawn_blocking` (`src/task/blocking.rs`), which clones `state`, `job_tx`, and `completed_tx`.
- **Removed by:** `ContextGuard::drop` (`src/context.rs`), which calls `uninstall()`.
- The interaction is one-directional: `Mar::run` installs, consumers read, the guard removes. No consumer installs or modifies the context.

### Source Links

- `src/context.rs` — the `ContextHandle`, `thread_local!`, `install`, `uninstall`, `with`, and `ContextGuard`.
- `src/mar.rs` — `run()`, which installs the context and declares the guard.
- `src/time/sleep.rs` — `sleep()`, which reads the context to get the timer registry.
- `src/task/blocking.rs` — `spawn_blocking()`, which reads the context to get the state, job sender, and completed sender.

---

## 3. Design Decisions

### Why `thread_local!` and Not a Global `OnceLock` or Static

A `thread_local!` variable holds a per-thread value. An alternative is a global `OnceLock<ContextHandle>` (a one-time-initializable static). This fails for three reasons.

First, re-entrancy. If `Mar::run` is called inside another `Mar::run` (a nested runtime), a global would be overwritten by the inner `run`, clobbering the outer runtime's handle. A thread-local is per-thread, so nesting works: the inner `run` installs its handle, the inner `run` finishes, `uninstall` restores `None`, and the outer `run` continues. (In practice, nesting is unusual, but the mechanism must not break if it happens.)

Second, `Rc` is not `Send`. The handle contains `Rc<RefCell<RuntimeState>>` and `Rc<TimerRegistry>`. `Rc` uses non-atomic reference counting, so it is not safe to send across threads. A `static` variable requires `Send + Sync` (or const-initializable data), so the compiler forbids `static CONTEXT: ContextHandle = ...`. `thread_local!` sidesteps this: the value never crosses a thread boundary, so `Send` is not required.

Third, multiple threads. If the program calls `Mar::run` on several threads simultaneously, each thread needs its own runtime handle. A thread-local gives each thread its own slot automatically.

### Why `Rc` Forces `thread_local!`

The `ContextHandle` holds `Rc<RefCell<RuntimeState>>` and `Rc<TimerRegistry>`. `Rc` is not `Send` or `Sync`. Any shared storage (`static`, `OnceLock`, `LazyLock`) requires `Send + Sync`. The compiler rejects `static CONTEXT: RefCell<Option<ContextHandle>>` because `ContextHandle` contains `Rc`. The `thread_local!` macro is the only standard-library mechanism that permits non-`Send` types in a static-like position.

### What Happens If a Worker Thread Calls `context::with()`

Worker threads never call `context::install`. Their thread-local slot is `None`. A call to `context::with()` panics with `"runtime context not installed"`. This is safe because worker threads run blocking closures (`Box<dyn FnOnce()>`), not futures. No future code — and therefore no call to `sleep` or `spawn_blocking` — executes on a worker thread. If a user's blocking closure tried to call `spawn_blocking` from inside a worker, the panic would be the correct signal that this is not supported.

### Ecosystem Compatibility

Because the mechanism is thread-local, any crate can check whether the runtime is installed by calling `context::with`. This is exactly how `time::sleep` and `task::spawn_blocking` work: they are plain functions that read the thread-local. No runtime parameter is needed. This means a library crate can provide async utilities that work with `mar` without `mar` being in scope — the library calls `context::with` and either succeeds (runtime is installed) or panics (no runtime, which is a programming error).

### When This Changes

A multi-threaded runtime (like tokio) uses a thread-local per worker thread. Each worker thread installs its own handle when it enters the runtime. Tokio's `Runtime::enter` is the same mechanism at larger scale: it installs a thread-local handle for the duration of a closure. The core idea — per-thread ambient state — does not change; only the set of types in the handle and the number of threads involved change.

---

## 4. Failure Modes and Misconceptions

### What Breaks If the Guard Is Declared Before the Runtime

If the code were:

```rust
let _context = context::install(context::ContextHandle { ... });
let mut runtime = Self::new();
```

`_context` would be declared first, so it would drop *after* `runtime` (reverse declaration order). `runtime` drops first: `WorkerPool::drop` calls `shutdown()`, which drops `self.job_tx.take()`. But the `ContextHandle` still holds a clone of `job_tx` from when `install` was called. The channel has a live sender, so `recv()` does not return `Err`. The workers block forever. `join()` blocks forever. `run()` never returns.

The tests `runtime_drop_joins_workers_promptly_after_run` and `panicking_root_future_still_joins_workers` in `src/mar.rs` assert that `run()` returns within 500 milliseconds. The prompt return *is* the assertion: if a sender leaked, `run()` would hang, the assertion would fail, and the test would time out.

### What Breaks If the Guard Never Runs `uninstall`

If the guard were forgotten (`std::mem::forget(_context)`), `Drop` would not run, and the thread-local would retain a stale `ContextHandle` after `run()` returns. A later call to `time::sleep` on the same thread would reach a dead runtime — the timer registry it references belongs to a `Mar` that no longer exists. The sleep would register with a registry nobody drains, and the future would never resolve. `std::mem::forget` is safe Rust, so this is not undefined behavior, but it is a logical bug that produces a hang.

### What Breaks If a `BlockingTask` Is Dropped Without Removing Its Waker

Each `BlockingTask` registers its waker in `state.blocking_wakers` on its first poll. If the `BlockingTask` is dropped (cancelled) before the worker finishes, the entry must be removed. `BlockingTask::Drop` does this. If it did not, the stale entry would keep `blocking_wakers` non-empty, `is_done` would never pass, and `run()` would never return — the executor would wait for a result that nobody will deliver.

### Common Misunderstandings

1. **"The context is global state."** It is per-thread. Each thread has its own `CONTEXT` slot. Two threads calling `Mar::run` simultaneously each get their own handle.

2. **"The guard holds the runtime."** The guard holds nothing. It is a zero-sized struct whose only purpose is to run `uninstall()` when it drops. The runtime is held by `Mar`, which lives in `run`'s local variable `runtime`.

3. **"`with()` panicking is a bug."** It is the designed behavior for misuse. Calling `sleep` or `spawn_blocking` outside `run()` is a programming error. A silent no-op would produce a future that never resolves, which is harder to debug than a panic with a clear message.

4. **"Each call to `sleep` installs a context."** `Mar::run` installs the context once at the start. Consumers (`sleep`, `spawn_blocking`) only read it. Installing a context on every call would be expensive and pointless — the handle does not change during `run`.

---

## 5. Summary

- `ContextHandle` is a struct holding clones of the runtime's scheduling state, timer registry, and worker-pool channels. It lives in a `thread_local!` slot so any code on the thread can reach it without a parameter.

- `Mar::run` installs the handle at the start and returns a `ContextGuard` that uninstalls it on drop. The guard must be declared *after* the runtime so it drops *first*, releasing the channel senders before the pool joins its workers.

- `context::with` reads the slot and panics if no runtime is installed. This panic is the designed response to calling `sleep` or `spawn_blocking` outside `run`.

- The design exists because `Rc` is not `Send`, so a global static is impossible, and because explicit parameter passing would infect every function signature and break ecosystem compatibility.
