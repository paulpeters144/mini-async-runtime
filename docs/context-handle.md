# ContextHandle: The Invisible Plumbing

## The Problem No One Wants to Talk About

An async function sits buried deep in a call stack. It needs to pause for a moment, so `time::sleep(Duration::from_secs(1))` is called. Or it needs to hand off a heavy computation, so `task::spawn_blocking(|| expensive_work())` is called. These feel natural — write them and they just work. But they conceal a wiring problem that is easy to miss.

Both `sleep` and `spawn_blocking` need access to pieces of the runtime that live inside the executor's struct: the timer registry, the worker pool's channel senders, the shared scheduling state. Those handles were never passed in. The function signature doesn't mention the runtime. Yet somehow, when the code actually runs, the timer gets registered in the shared registry and the closure finds its way to a worker thread. There's no visible thread connecting them.

The naive approach would be to thread a runtime handle through every function signature — every `async fn` that might need to sleep or spawn would take it as a parameter. This demolishes itself immediately. Every intermediate function that calls an async function has to forward the handle. Because `.await` expands into state-machine fields, the handle must be stored across yield points, which imposes `'static` bounds that a borrowed reference can never satisfy. And even if all the type-system problems were somehow solved, no Rust async library in the ecosystem expects a handle to be passed. Tokio's `sleep` is a bare free function. So is smol's, and async-std's, and every other runtime's. The code would not compose with anyone else's.

So how do deeply nested async functions reach the runtime's internal services without a handle threaded through every function signature? The answer is ambient, thread-local state — a handle stashed somewhere every function on the thread can reach, invisible to type signatures but present when the code actually executes.

## The Answer: Ambient State

While `Mar::run` is executing, the runtime stores a handle to itself in a `thread_local!` slot. Any code on that thread can read the handle without receiving it as an argument. It's like a global variable, but scoped to one thread and one `run()` call.

This is exactly what Tokio does with its `CURRENT` runtime handle. Smol uses the same trick with a thread-local executor reference. The mechanism is always the same: install a handle when the runtime starts, remove it when the runtime stops, and let any code on the thread access it in between.

## The Struct

```rust
pub(crate) struct ContextHandle {
    pub state: Rc<RefCell<RuntimeState>>,
    pub wheel: Rc<TimerRegistry>,
    pub job_tx: mpsc::Sender<Job>,
    pub completed_tx: mpsc::Sender<BlockingId>,
}
```

Four fields, each a clone of a handle the executor already owns. `ContextHandle` doesn't own anything — it holds shared references to the runtime's internal structures.

**`state`** — the scheduling state. When `spawn_blocking` needs to allocate a `BlockingId` or register a waker, it borrows this. The executor holds its own `Rc` to the same `RuntimeState`.

**`wheel`** — the timer registry. When `time::sleep` builds a `Sleep` future, it clones this `Rc` so the future can register itself with the shared registry on its first poll.

**`job_tx`** — the sending end of the channel that carries closures to the worker pool. When `spawn_blocking` submits a job, it clones this sender and pushes the closure through.

**`completed_tx`** — the sending end of the channel that carries `BlockingId`s from workers back to the executor. The job closure inside `spawn_blocking` gets a clone of this sender so it can report which job just finished.

## How It Lives on a Thread

The storage is simple:

```rust
thread_local! {
    static CONTEXT: RefCell<Option<ContextHandle>> = const { RefCell::new(None) };
}
```

`thread_local!` declares a static variable whose value is per-thread. Each thread gets its own copy. The type is `RefCell<Option<ContextHandle>>` — an optional handle wrapped in interior mutability.

Why `RefCell`? Because the slot needs to be both readable (to look up the handle) and writable (to install and uninstall it). `RefCell` provides that runtime-checked interior mutability on a single thread.

Why `Option`? Because there are two states. When `Mar::run` is executing on this thread, the slot holds `Some(handle)`. Any other time — before the runtime starts, after it shuts down, or on a worker thread — the slot holds `None`.

## Installing and Uninstalling

### Install

```rust
pub(crate) fn install(handle: ContextHandle) -> ContextGuard {
    CONTEXT.with(|c| *c.borrow_mut() = Some(handle));
    ContextGuard
}
```

One line of work: overwrite the thread-local `Option` with `Some(handle)`. If a previous handle was installed (rare — only if `Mar::run` is nested inside `Mar::run`), it's dropped here.

The return value is a `ContextGuard` — a zero-sized struct with no fields. Its entire reason for existing is its `Drop` impl:

```rust
pub(crate) struct ContextGuard;

impl Drop for ContextGuard {
    fn drop(&mut self) {
        uninstall();
    }
}
```

When `ContextGuard` is dropped, it calls `uninstall()`. Rust guarantees `Drop::drop` runs when a value goes out of scope — whether through normal return or during a panic unwind. This means the thread-local is always cleared, no matter how `Mar::run` exits.

### Uninstall

```rust
pub(crate) fn uninstall() {
    CONTEXT.with(|c| *c.borrow_mut() = None);
}
```

Sets the slot back to `None`, releasing the `ContextHandle` and all the handles it holds. After uninstall, any call to look up the context will panic — there's no runtime to provide services.

### Reading

```rust
pub(crate) fn with<F, R>(f: F) -> R
where
    F: FnOnce(&ContextHandle) -> R,
{
    CONTEXT.with(|c| f(c.borrow().as_ref().expect("runtime context not installed")))
}
```

`with` reads the thread-local and calls a closure with a reference to the installed handle. The `.expect("runtime context not installed")` is the panic that fires when `time::sleep` or `task::spawn_blocking` is called outside of `Mar::run`.

This panic is a feature, not a bug. There's no meaningful default behavior for a `sleep` with no timer registry — it would produce a future that never resolves. A panic with a clear message tells exactly what went wrong: the runtime context was never installed.

## The Drop-Order Subtlety

This is the most important detail in the entire codebase — the one that, if gotten wrong, deadlocks the runtime.

In `Mar::run`:

```rust
let mut runtime = Self::new();

let _context = context::install(context::ContextHandle {
    state: runtime.state.clone(),
    wheel: runtime.wheel.clone(),
    job_tx: runtime.pool.job_tx(),
    completed_tx: runtime.pool.completed_tx(),
});
```

`runtime` is declared first. `_context` is declared second. Rust drops locals in *reverse* declaration order, so `_context` drops first, then `runtime`.

When `_context` drops, `ContextGuard::drop` calls `uninstall()`, which sets the thread-local to `None`. The `ContextHandle` that was stored there — including its clones of `job_tx` and `completed_tx` — is released.

Then `runtime` drops. `WorkerPool::drop` calls `shutdown()`, which drops the original `job_tx` sender, closing the job channel. Each worker's `recv()` returns `Err` (channel closed), the workers exit, and `join()` completes. Clean shutdown.

Now imagine the order were reversed — `_context` declared first, `runtime` second. `runtime` would drop first. `WorkerPool::shutdown` would try to close the job channel. But the `ContextHandle` *still* holds a clone of `job_tx`. The channel would not close because there's still a live sender. Every worker's `recv()` would block forever waiting for a job that never comes. `join()` would never return. `run()` would hang.

The guard's position in the declaration order is not cosmetic. It's the difference between a clean shutdown and an infinite deadlock.

## How Leaf Futures Use the Context

### `time::sleep`

```rust
pub fn sleep(duration: Duration) -> Sleep {
    let registry = crate::context::with(|ctx| ctx.wheel.clone());
    Sleep {
        registry,
        deadline: Instant::now() + duration,
        id: None,
        done: false,
    }
}
```

Three lines. Read the thread-local, clone the timer registry, build a `Sleep`. The `Sleep` future holds the registry and will register itself on its first poll. No parameter needed.

### `task::spawn_blocking`

```rust
let (state, job_tx, completed_tx) = context::with(|ctx| {
    (ctx.state.clone(), ctx.job_tx.clone(), ctx.completed_tx.clone())
});
```

Reads three fields from the context, clones them, and uses them to allocate a `BlockingId`, submit the closure to the worker pool, and build a `BlockingTask` future. Again, no parameter.

### What Happens Outside `run()`

If `time::sleep(duration)` is called without `Mar::run` wrapping it:

```rust
fn main() {
    let _ = time::sleep(Duration::from_secs(1)); // panics here
}
```

`context::with` finds `None` in the thread-local. `.expect("runtime context not installed")` panics with that exact message. The alternative — making `sleep` a no-op and returning a future that silently hangs — would be much harder to debug.

## Design Choices

**Why `thread_local!` and not a global `OnceLock`?** Three reasons. First, re-entrancy: `Mar::run` calls can be nested, and a thread-local keeps them independent. A global would be overwritten by the inner call. Second, `Rc` is not `Send`, so a global `static` won't compile — `thread_local!` is the only standard-library mechanism that permits `!Send` types in a static position. Third, multiple threads: if the program runs `Mar::run` on several threads simultaneously, each needs its own handle. Thread-locals provide that automatically.

**Why does the guard hold no data?** It's a sentinel. Its only purpose is to call `uninstall()` on drop. It doesn't need to hold the handle because the handle is stored in the thread-local, not in the guard.

**What if a worker thread calls `context::with`?** Worker threads never call `install`. Their thread-local is `None`. A call to `context::with` from a worker panics — but that's correct, because worker threads run blocking closures, not futures. There's no legitimate reason for `sleep` or `spawn_blocking` to be called from a worker.

## Common Misconceptions

**"The context is global state."** It's per-thread. Each thread has its own `CONTEXT` slot. Two threads running `Mar::run` simultaneously each get their own handle.

**"The guard holds the runtime."** The guard is a zero-sized marker. The runtime is held by `Mar`, which lives in `run`'s local variable `runtime`. The guard's only job is to call `uninstall()`.

**"`with()` panicking is a bug."** It's designed behavior. Calling `sleep` or `spawn_blocking` outside `run()` is a programming error, and a panic with a clear message is the fastest way to find the bug.

**"Every call to `sleep` installs a context."** The context is installed once at the start of `run()`. Consumers like `sleep` and `spawn_blocking` only read it. Installing on every call would add pointless overhead.

## Summary

`ContextHandle` solves the "how do nested futures reach the runtime" problem with ambient thread-local state. It holds clones of the runtime's scheduling state, timer registry, and worker-pool channels. `Mar::run` installs it at the start, a `ContextGuard` uninstalls it on drop, and leaf futures read it through `context::with`. The guard's drop order — before the runtime — is the detail that prevents the worker pool from deadlocking on shutdown.

Source: `src/context.rs`
