# RuntimeState: The Shared Memory of the Executor

## What Does an Executor Need to Remember?

Building a scheduler from scratch requires tracking a lot of state. Tasks appear, run, pause, and eventually finish. Some tasks are actively being polled right now; others are parked, waiting for a timer to expire or a worker thread to finish a job. Wakers fire from outside the executor — a socket becomes readable, a deadline passes — and they need to tell the scheduler which parked task to re-queue. Every piece of this must be tracked accurately, because losing track of any one piece means something silently hangs.

The absolute minimum needed is three kinds of memory. First, a list of "who is ready to be polled right now," so that on each pass through the event loop the executor knows exactly which doors to knock on. Second, a place that holds every task that still exists in the system, running or parked, so any of them can be looked up when their turn comes. Third, a way to tell tasks apart — a unique name for each one — so that when a waker fires from some external source, it can identify itself unambiguously and the wrong task is never accidentally polled.

This runtime adds a fourth kind of memory for blocking work. When a task submits a closure to the worker pool and awaits the result, there needs to be a connection between the worker's completion signal and the specific task that's waiting. A separate map ties blocking-job identifiers to the wakers of the tasks that are waiting on them, keeping the two identity spaces — async tasks and blocking jobs — cleanly separated.

All four kinds of memory live in one struct: `RuntimeState`. Every component in the system that schedules, wakes, or submits work — the executor loop, the wakers, the `spawn_blocking` path — reads and writes this single shared structure. To understand how information flows through the runtime, this is the map.

## The Struct

```rust
pub struct RuntimeState {
    pub queue: Arc<Mutex<Vec<TaskId>>>,
    pub tasks: HashMap<TaskId, Task>,
    pub next_id: TaskId,
    pub blocking_wakers: HashMap<BlockingId, Waker>,
    pub(crate) next_blocking_id: BlockingId,
}
```

Five fields, two concerns. The first three manage async tasks. The last two manage blocking work. Let's take them one at a time.

### The Ready Queue: `queue: Arc<Mutex<Vec<TaskId>>>`

A `Vec` of `TaskId`s. When a task is spawned, its id goes here. When a waker fires, its id goes here. The executor drains it every iteration, polling each task in order.

Why ids and not the tasks themselves? A `Task` owns its future, and a future must exist in exactly one place — it can't be duplicated across the queue and the table. The queue is just "who needs polling now," and ids make that one word per entry.

The queue is FIFO: `queue.remove(0)` pops from the front. Tasks are polled in the order their ids were pushed. It's simple, predictable, and good enough for a single-threaded runtime.

**Why `Arc<Mutex<...>>`?** The wakers need to share the queue. Every `TaskWaker` holds an `Arc` to the same queue, so when a waker fires, it pushes into the same `Vec` that the executor drains. `Arc` gives shared ownership across all waker clones; `Mutex` makes the `Vec` `Sync`, which the `Wake` trait contract requires.

The lock is never contended (single-threaded runtime), so `lock().unwrap()` is always instant. The `Mutex` exists to satisfy the type system, not to prevent data races.

### The Task Table: `tasks: HashMap<TaskId, Task>`

This is the "alive" set. Every task that exists — whether it's running, parked, or somewhere in between — is in this map. Presence in the table means "this task is alive." Presence in the queue means "and it needs polling *now*."

When the executor polls a task, it removes it from the table first, polls it, then either re-inserts it (if it returned `Pending`) or drops it (if it returned `Ready`). A parked task lives in the table and waits for its waker.

`HashMap` gives O(1) lookup by id — the executor pops an id from the queue, looks it up here, and finds the task in constant time. Insert and remove are also O(1). Tasks complete in arbitrary order, not FIFO, so a `Vec` wouldn't work — the entire list would need to be scanned to find the right task.

### The ID Counter: `next_id: TaskId`

Every spawned task gets a unique, never-reused id. `spawn_root` reads the counter, uses the value, and increments:

```rust
let id = state.next_id;
state.next_id.0 += 1;
```

The counter only goes up. An id is never recycled.

Why monotonic? Wakers outlive tasks. A stale waker — held by a timer that fired after its task completed, or by a socket that outlived the task — can fire long after its task is gone. If ids were reused, a stale wake for `TaskId(5)` might land on a completely different task that was assigned `TaskId(5)` after the original was dropped. The executor would look up the right id and poll the wrong future.

Monotonic ids make every id unambiguous for the lifetime of the runtime. A stale wake for `TaskId(5)` finds no entry in the table, and the drain skips it.

### The Blocking-Waker Map: `blocking_wakers: HashMap<BlockingId, Waker>`

Task-waker-id tracking, but for blocking work. When `spawn_blocking` sends a closure to the worker pool, it allocates a `BlockingId` — a separate identity space from `TaskId`. The future that awaits the result (`BlockingTask`) registers its waker in this map under that id.

When a worker finishes a job, it sends the `BlockingId` through a completed channel. The executor looks up that id here, finds the waker, and wakes the task. The waker pushes the *task's* id onto the ready queue — blocking ids and task ids are different types, different spaces, different maps.

A waker must be registered here for the runtime to finish. `is_done` checks `blocking_wakers.is_empty()` — if there's a single stale entry, `run()` never returns. Two code paths remove entries: `BlockingTask::poll` when the result arrives, and `BlockingTask::Drop` when the task is cancelled.

### The Blocking ID Counter: `next_blocking_id: BlockingId`

Same idea as the task id counter, but for blocking jobs. It's a separate counter because blocking ids are a separate identity space. Spawning a task must not consume a blocking id, and spawning a blocking job must not consume a task id.

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TaskId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockingId(pub usize);
```

They're both `usize` under the hood, but they're different types. The compiler won't allow a `BlockingId` to be passed where a `TaskId` is expected, or vice versa. This is a small thing, but it eliminates an entire class of bugs — a blocking id will never accidentally be looked up in the task table.

## How Borrows Are Managed

All five fields sit behind a `RefCell` (the whole struct is wrapped in `Rc<RefCell<RuntimeState>>`). `RefCell` allows mutation through a shared reference, with borrows checked at runtime. The rule is: one mutable borrow at a time, or many shared borrows, never both.

The defense against double-borrow panics is scope discipline. Every borrow is confined to the smallest block that needs it, and no user code ever runs while a borrow is held.

Here's the key pattern from `drain_ready_queue`:

```rust
// Borrow, pop an id, release.
let next = {
    let state = runtime.state.borrow_mut();
    let mut queue = state.queue.lock().unwrap();
    if queue.is_empty() { None } else { Some(queue.remove(0)) }
};

// Short borrow to remove the task.
let Some(mut task) = runtime.state.borrow_mut().tasks.remove(&id) else {
    continue;
};

// No borrow held during poll — user code can spawn, sleep, etc.
match task.poll(&mut cx) {
    Poll::Pending => {
        runtime.state.borrow_mut().tasks.insert(id, task);
    }
    Poll::Ready(()) => {}
}
```

The `{ }` block around the queue pop releases the borrow before anything else happens. The task removal is a single-statement borrow. The poll runs with no borrow at all — important, because the poll's user code might call `spawn_blocking`, which needs its own `borrow_mut()` on the same `RefCell`. If the drain held a borrow across the poll, that would panic. The scoped blocks are the defense.

The same pattern appears everywhere: `spawn_blocking` allocates its id inside a `{ }` block and releases before returning. `wake_completed_blocking` clones the waker out of the map inside a `{ }` block and releases before calling `wake_by_ref()`.

## Walking Through a Full Scheduling Cycle

Let's trace `Mar::run(async { task::spawn_blocking(|| 21 * 2).await })` and watch all five fields change at each step.

**Initial state:** queue `[]`, tasks `{}`, next_id `TaskId(0)`, blocking_wakers `{}`, next_blocking_id `BlockingId(0)`.

**1. Spawn root.** `spawn_root` reads `TaskId(0)`, bumps next_id to `TaskId(1)`. Task inserted, id pushed.
→ queue `[TaskId(0)]`, tasks `{TaskId(0): root}`, next_id `TaskId(1)`

**2. First drain.** Pops `TaskId(0)`, removes task, polls. The root future calls `spawn_blocking`. It reads `BlockingId(0)`, bumps next_blocking_id to `BlockingId(1)`. The job is sent. `BlockingTask::poll` inserts the waker under `BlockingId(0)` and returns `Pending`. Root returns `Pending`. Task re-inserted.
→ queue `[]`, tasks `{TaskId(0)}`, blocking_wakers `{BlockingId(0): waker_for_TaskId(0)}`

**3. Park.** `is_done`: tasks not empty → false. The executor parks. No timers, so `None` timeout (park until woken).

**4. Worker completes.** Worker runs `21 * 2 = 42`, sends result, sends `BlockingId(0)`, calls `wake()`. Executor unparks.

**5. Process completion.** `wake_completed_blocking` drains `[BlockingId(0)]`, finds the waker, calls `wake_by_ref()`. Task id pushed.
→ queue `[TaskId(0)]`, blocking_wakers still has the entry (waking doesn't remove)

**6. Second drain.** Pops `TaskId(0)`, polls. `BlockingTask::poll` finds `Ok(Ok(42))`, removes the `BlockingId(0)` entry, returns `Ready(42)`. Root completes. Task dropped.
→ queue `[]`, tasks `{}`, blocking_wakers `{}`

**7. Done.** `is_done` sees all three conditions met. `run()` returns.

## Design Choices

**Why `Rc<RefCell<RuntimeState>>` instead of `Arc<Mutex<...>>`?** The runtime is single-threaded. `Rc` is cheaper than `Arc` (no atomic reference counting). `RefCell` panics immediately on a double-borrow with a clear message and line number; a `Mutex` would deadlock silently, with no message and no backtrace — much harder to debug. `Rc` also refuses to be sent across threads, so the single-threaded invariant is enforced by the compiler.

**Why separate id types for tasks and blocking work?** `TaskId` and `BlockingId` are distinct types despite both being `usize`. The compiler prevents mixing them. A shared counter or a single id type would lose this compile-time safety and make every mixed use a runtime bug.

**Why is the queue in `RuntimeState` and not directly on `Mar`?** Because the queue needs to be shared with wakers as an `Arc`, and wakers shouldn't need a reference to the entire `Mar` struct. By keeping all scheduling state in one place, the `ContextHandle` carries a single pointer (`state`) that gives `spawn_blocking` and other leaf futures access to everything they need.

## Common Misconceptions

**"The ready queue holds tasks."** It holds ids — one word each. The tasks live in the table. The queue is just "who needs polling now."

**"The `Mutex` prevents races."** There are no races; the runtime is single-threaded. The `Mutex` exists only to make `Vec` `Sync`, which the `Wake` trait requires for `Waker::from(Arc<T>)`.

**"A `Pending` task is lost."** It's parked: in the table, out of the queue, waiting for its waker. Losing a task is what happens when a future returns `Pending` *without* storing a waker — the table is the safety net, not the problem.

**"Ids can be reused after a task completes."** They're never reused. A stale waker can fire after its task is gone, and monotonic ids guarantee that firing won't land on a different task.

## Summary

`RuntimeState` is the single structure that holds the runtime's scheduling memory. The ready queue tells the executor what to poll. The task table holds everything that's alive. The id counters give every piece of work a permanent, unique name. And the blocking-waker map connects worker-completions back to the tasks that are waiting for them.

Every component that schedules — the executor, the wakers, `spawn_blocking` — reads and writes this one struct. It's the shared truth that makes "poll me again later" a solvable problem.

Source: `src/runtime_state.rs`
