# Wakers: How a Paused Task Gets Back in Line

## The Question

A future has returned `Pending`. The executor has parked it in the task table and removed its id from the ready queue. The task's progress is preserved — its state machine remembers exactly where it left off — but the executor will not visit it again on its own. Something external to the task, something that knows which task is waiting and has a channel to reach the executor, must raise its hand and say "this one, now." Without that signal, a future that returns `Pending` is functionally gone, alive in memory but never polled again, its `.await` points suspended forever.

The waker is the mechanism that closes this gap. It's a compact, cloneable handle whose entire purpose is to say, on behalf of a specific task, "try polling me again." It knows one thing — which task it belongs to — and it knows one route — a shared queue that the executor checks every iteration. No callbacks register, no polling logic lives inside it, and it has no knowledge of what code the task actually runs. It's a slip of paper with a task number on it, dropped into a bin the executor empties on each pass through the loop.

## What the Standard Library Provides

Rust provides `std::task::Waker` — an opaque, cloneable handle that's `Send + Sync`. It isn't built by hand. The safe `Wake` trait is implemented instead:

```rust
pub trait Wake {
    fn wake(self: Arc<Self>);
    fn wake_by_ref(self: &Arc<Self>) { ... }
}
```

`wake(self: Arc<Self>)` consumes an `Arc` of the payload — it's called when the waker can be destroyed after waking. `wake_by_ref` borrows it instead, so a stored waker can fire many times (its default implementation clones the `Arc` and calls `wake`).

The conversion is `Waker::from(Arc<T>)` where `T: Wake + Send + Sync + 'static`. Std builds the vtable, the clone logic, and the reference-counted drop automatically. No `unsafe`, no hand-rolled function pointers.

## This Runtime's Waker

```rust
pub struct TaskWaker {
    queue: Arc<Mutex<Vec<TaskId>>>,
    id: TaskId,
}
```

Two fields. That's it.

**`queue`** is a shared reference to the runtime's ready queue — the same `Vec<TaskId>` that `Mar::run` drains every iteration. `Arc` means every waker clone shares the same queue. `Mutex` is required not because there's contention (the runtime is single-threaded!), but because `Waker::from(Arc<T>)` demands `T: Sync`, and `Mutex` is how a plain `Vec` satisfies that. The lock is never actually contested — `lock().unwrap()` is always instant.

**`id`** is which task this waker should re-queue. It's the waker's entire identity. Two wakers differ only in this field. If the waker holds `TaskId(3)` and its `queue` points at the runtime's shared ready queue, then calling `wake()` is exactly "push `TaskId(3)` onto the shared queue."

### The One Method That Matters

```rust
impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.lock().unwrap().push(self.id);
    }
}
```

One line. Lock. Push. Done. The waker doesn't check whether the task still exists. It doesn't verify that the queue belongs to an active runtime. It doesn't poll anything. It puts an id into a `Vec` and walks away.

This simplicity is deliberate. The executor owns all the complexity — looking up tasks, polling them, deciding if they're done. The waker is a pure signal: "task N might be ready, please check."

## The Six Phases of a Waker's Life

**1. Creation.** `spawn_root` builds the waker when the task is born:

```rust
let waker = TaskWaker::new(state.queue.clone(), id);
```

It clones the ready queue's `Arc` and stores the task's id.

**2. Storage.** The `Task` keeps this waker in its `waker` field — the canonical copy.

**3. Retrieval.** `drain_ready_queue` clones it each time the task is polled:

```rust
let waker = task.waker().clone();
let mut cx = Context::from_waker(&waker);
```

**4. Propagation.** Inside the task's `poll`, leaf futures clone the waker out of `cx` and hand it to whatever will eventually wake them. `Sleep::poll` stores it in the timer registry. `BlockingTask::poll` stores it in the blocking-waker map. The leaf future says "here's how to reach me" and returns `Pending`.

**5. Firing.** The external condition happens. A deadline passes, a worker finishes a job. The driver calls `wake_by_ref()`. One lock, one push. The task's id lands back on the ready queue.

**6. Response.** The executor's next drain pops the id and polls the task. The refcount on the waker's `Arc` ticks down as stored clones are cleaned up.

## Walking Through a Wake

Trace a `spawn_blocking` example from the waker's perspective. The root task is `TaskId(0)`. There's a blocking job tracked by `BlockingId(0)`.

1. **Task spawns.** `TaskWaker::new(queue_clone, TaskId(0))` creates the waker. It lives on the task.
2. **Task polls.** The executor clones the waker into a `Context`. Inside the poll, `BlockingTask::poll` clones it again and stores it under `BlockingId(0)` in `blocking_wakers`. The future returns `Pending`. The task parks.
3. **Worker finishes.** The worker sends the result and calls `wake()` on the pool's `mio::Waker`. The executor unparks, sees the wake token, and calls `wake_completed_blocking`.
4. **Waker fires.** `wake_completed_blocking` finds the waker stored under `BlockingId(0)` and calls `wake_by_ref()`. The `Wake` impl runs: lock, push `TaskId(0)`. Queue becomes `[TaskId(0)]`.
5. **Executor responds.** Next drain pops `TaskId(0)`, polls the task. `BlockingTask::poll` reads the result channel, finds the value, returns `Ready`.

The waker's entire contribution to this orchestration is the push in step 4. Everything else — storing it, looking it up, acting on its signal — is done by other components.

## Why Not Just Poll Directly?

A natural question: why doesn't `wake()` just poll the task itself? It has the id, it has the queue — it could remove the task from the table and call `poll` right there.

Two reasons not to:

**Separation of concerns.** The waker is a signal; the executor is the thing that acts on signals. If the waker polled directly, every component that fires a waker — the timer registry, the worker pool completion path — would need its own copy of the poll-logic (remove, context-build, poll, re-insert). Duplicated code, duplicated bugs.

**Reentrancy is dangerous.** The executor might be in the middle of polling *another* task when a wake fires. If the wake tried to poll directly, there would be a poll happening inside a poll — reentrant execution of the runtime loop, while the borrow of `RuntimeState` is still held. That's a `RefCell` double-borrow panic at best, a logic error at worst.

Pushing onto a queue defers the poll to the top of the next drain iteration, when the executor is in a clean, consistent state with no borrows held.

## Design Choices

**Why `Arc<Mutex<Vec<TaskId>>>` and not `Rc<RefCell<...>>`?** The `Wake` trait contract requires `Send + Sync` on the waker payload. `Rc` is `!Send`, so the compiler rejects it. `Arc` is `Send + Sync` when the inner type is `Sync`, and `Mutex` makes `Vec` `Sync`. Even though the runtime is single-threaded and the lock is never contended, the type system needs this guarantee because a stored waker *could* be moved to another thread (worker threads hold wakers inside `catch_unwind` for example). The `Arc` is the only way to satisfy the trait bounds.

**What if a waker fires after its task is gone?** This is a spurious wake — a timer or a worker delivers a wake for a task that already completed and was dropped. The waker pushes the stale id anyway. The drain does `let Some(mut task) = tasks.remove(&id) else { continue; }` — the lookup fails, and the id is skipped. No crash, no wrong task polled. The runtime just moves on. Spurious wakes are a natural consequence of the id-based design, and they're harmless because the drain verifies every id before polling.

**Why `Arc` and not `Weak` for the queue?** A stale waker with a `Weak` reference to the queue would silently fail — the upgrade would return `None`, and the wake would be lost with no trace. An `Arc`-based waker keeps the queue alive even after the runtime has dropped its reference. The test `waker_keeps_queue_alive` verifies this: a surviving waker can still push to the queue after everything else is gone. The push succeeds, and when the last waker drops, the queue is finally freed.

## Common Misconceptions

**"`wake()` runs the task's code."** It runs exactly one line: `self.queue.lock().unwrap().push(self.id)`. The task's code runs later, when the executor drains the queue and polls it.

**"A waker is a callback."** A callback is a function given to something, and that something calls the function when it's ready. A waker is different — it's a handle to a shared data structure (the queue). Calling `wake()` is data manipulation (push), not function invocation. The executor polls the task because it *found* the id in the queue, not because the waker told it to.

**"Waking twice is a bug."** Waking twice pushes the id twice, so the executor polls twice. If the task was already done, the second poll finds it missing from the table and skips it. If it wasn't done, the second poll returns `Pending` again. Correctness requires at least one wake, not exactly one.

**"The waker knows what the future is."** The waker knows one thing: `TaskId`. It has no idea what code the task runs, whether it's a `Sleep`, a `BlockingTask`, or a deeply nested `async fn`. Finding the task, polling it, and deciding it's done — that's all the executor's job.

## Summary

A waker is a two-field struct whose entire job is pushing a `TaskId` into a shared `Vec`. It's the thread of communication that connects the thing a task is waiting for (a timer, a worker thread, an I/O socket) back to the executor. It exists because a `Pending` future can't resume itself — something external must request the re-poll — and the standard library's `Wake` trait is the uniform interface that makes this possible.

Source: `src/waker.rs`
