# Runtime State: The Shared Scheduling State

## 1. Concept: What Scheduling State Does a Runtime Need?

### The Core Concept

An executor must know three things to schedule work: which work is ready to run right now, which work exists at all, and how to tell pieces of work apart. In this runtime, one struct — `RuntimeState` in `src/runtime_state.rs` — holds all three: a ready queue of ids, a table of live tasks, and an id counter that never reuses a number. It also holds a second id counter and a second map for blocking work, which is not made of futures and needs its own identity. Every component that schedules reads and writes this one struct, and all of them must see the same data.

To say why these are the three necessary kinds of bookkeeping, define the terms first. A **future** is a value that represents asynchronous work in progress; polling it advances the work and returns `Poll::Ready(value)` when it is done or `Poll::Pending` when it must be polled again later. A **task** is the runtime's container for one future, the struct in `src/task/mod.rs`: an id, a waker, and a `Pin<Box<dyn Future<Output = ()>>>`. A **waker** is the standard-library handle `std::task::Waker`, a cloneable value whose `wake()` method requests that some specific task be polled again.

**The minimum set.** Any executor, whatever its details, needs the following three, and this runtime has one field for each.

*The ready queue.* The executor must know which work is ready to run right now. In this runtime that is a `Vec<TaskId>` shared through an `Arc<Mutex<...>>`, holding not tasks but their ids. Work is put in the queue when it is spawned and when its waker fires; the executor drains the queue and polls each entry. Without a ready queue, the executor would have no way to know anything needs polling, and a task that returned `Pending` would never be revisited.

*The task table.* The executor must know which work exists at all. This runtime keeps a `HashMap<TaskId, Task>` of every live task. A task that returned `Pending` is not in the queue; it is parked here, alive but not scheduled, waiting for its waker to push its id back onto the queue. Without the table, a parked task would have to be dropped, and its progress with it.

*The id counter.* The executor must be able to tell pieces of work apart. A waker cannot hold a task or a future — a task owns its future, so the waker would duplicate it — so the waker holds a name, a `TaskId`. The table is keyed by that name, and the counter `next_id` guarantees every name is unique for the lifetime of the runtime. Without a stable id, a wake could not say which task it referred to.

### The Problem

A runtime that skips this bookkeeping fails as soon as a task returns `Pending`. Here is the naive shape:

```rust
use std::task::Waker;

// Naive: the ready structure holds the tasks themselves, and a waker
// re-queues by pushing a whole task. There is no id, no table, no counter.
let mut ready: Vec<Task> = Vec::new();
let mut parked: Vec<Task> = Vec::new();
```

This fails in three places, each tracing to a missing kind of bookkeeping.

First, the waker cannot name its task. A `Task` owns its future, so a waker cannot hold a copy of the task to push back; doing so would duplicate the future, and a future must exist in exactly one place. The waker must therefore hold a name, and the runtime must be able to answer "which task is this name?" That requires an id and a table, which is precisely the bookkeeping the naive design omits. Without them, a woken task cannot be found.

Second, the parking move is impossible without a lookup. A task that returns `Pending` must leave `ready` and wait in `parked` until its waker fires. To move it back, the runtime must find the right entry in `parked` — again a name-based lookup that `Vec` cannot do.

Third, nothing distinguishes one piece of work from another. Two tasks polled with no ids are interchangeable to any waker; a wake meant for one would be indistinguishable from a wake meant for the other. The id counter is what makes each unit of work addressable.

Blocking work adds a second problem with the same shape. A blocking closure — the kind `task::spawn_blocking` sends to the worker pool — is not a future and has no waker and no task of its own. When a worker finishes a job, the runtime must wake the *task* that is awaiting that job, not some other task and not all tasks. So the runtime keeps a separate map from `BlockingId` to that task's waker, plus a separate counter to mint blocking ids. This is the same identity-and-lookup mechanism as the task side, built for a kind of work that has no future.

### Why Any Runtime Needs This

The state is shared because the components that touch it are all different. The executor drains the queue. Wakers push ids onto the queue. `spawn_root` inserts tasks and advances the id counter. `spawn_blocking` registers wakers in the blocking map. If each of these kept its own copy of the data, a wake pushed into one queue while the executor drained another would silently lose work. On a single thread, "shared" means one shared pointer plus interior mutability: `Rc<RefCell<RuntimeState>>`. `Rc` is a non-atomic reference-counted pointer, so several owners can hold the same allocation. `RefCell` allows the fields inside to be mutated through a shared reference, with borrows checked at runtime.

Every async runtime has this same shape. Tokio keeps a run queue of ready tasks and a slab of task entries keyed by `TaskId`; its blocking thread pool tracks in-flight blocking work separately. async-std and smol do the same with their own structures. The names differ, but the three kinds of bookkeeping — ready queue, live-task table, id counter — and a fourth for blocking work, appear in all of them. `RuntimeState` is not a mar-specific quirk; it is the minimal set of state that makes "poll me again later" answerable.

## 2. How This Runtime Implements RuntimeState

### The Struct

The whole struct is in `src/runtime_state.rs`:

```rust
pub struct RuntimeState {
    pub queue: Arc<Mutex<Vec<TaskId>>>,
    pub tasks: HashMap<TaskId, Task>,
    pub next_id: TaskId,
    pub blocking_wakers: HashMap<BlockingId, Waker>,
    pub(crate) next_blocking_id: BlockingId,
}
```

The two id types it uses are also defined here:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TaskId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockingId(pub usize);
```

`TaskId` and `BlockingId` are newtypes over `usize`: a plain number wrapped in a distinct type so the compiler refuses to pass one where the other is expected. Each derives `Hash`, which the `HashMap` keys need, and `Eq`, `Clone`, and `Copy` so ids can be compared, copied out of the map, and stored in queues. A `BlockingId` is the same shape as a `TaskId` but is deliberately a different type, because a blocking job and a task are different kinds of work and must not be confused.

### Field by Field

**`queue: Arc<Mutex<Vec<TaskId>>>`** — the ready queue. It stores ids, not tasks, and that is the load-bearing choice. A task's future lives in exactly one place, the task table; a task owns its future and cannot be copied, so the queue cannot hold tasks. The queue is just "who needs polling now", and ids make it one word per entry. The queue is FIFO: `drain_ready_queue` in `src/mar.rs` pops with `queue.remove(0)`, so tasks are polled in the order their ids were pushed. `Arc` is an atomic reference-counted pointer, the sharing type that works across threads. The queue must be `Arc` because every waker holds it: the waker struct `TaskWaker` in `src/waker.rs` is `{ queue: Arc<Mutex<Vec<TaskId>>>, id: TaskId }`, and wakers are built with `Waker::from(Arc<T>)`, which requires the payload `T` to be `Send + Sync`. `Mutex` is a mutual-exclusion lock; the queue holds one solely to make the `Vec` `Sync`, a requirement the `Wake` contract imposes. The runtime is single-threaded, so the lock is never contended: `lock()` and `unwrap()` are trivial operations. The queue is written by `spawn_root`, which pushes the new task's id, and by every waker, whose `wake` pushes its id. It is read only by the drain, which pops ids.

**`tasks: HashMap<TaskId, Task>`** — the task table, the set of all live tasks. This is the parking model made concrete: presence in the table means "this task is alive", presence in the queue means "this task needs polling now". A `Pending` task is in the table and out of the queue. The map gives O(1) lookup by id, which the drain uses on every pop, and arbitrary insert and remove, which completion requires. The table is written by `spawn_root` (insert on spawn), read by `drain_ready_queue` (remove before poll, re-insert on `Pending`), and read by `is_done`, which checks `tasks.is_empty()` as one third of its termination test.

**`next_id: TaskId`** — the task id counter. `spawn_root` in `src/mar.rs` reads it and increments it in the same borrow:

```rust
let id = state.next_id;
state.next_id.0 += 1;
```

The counter only ever increases, so an id is never reused. Reuse would be dangerous because wakers outlive tasks: a stale waker held by a timer or a socket can fire long after its task completed. If its id were reused for a new task, the executor would look up the id and poll the *wrong* future. Monotonicity keeps every id unambiguous for the lifetime of the runtime.

**`blocking_wakers: HashMap<BlockingId, Waker>`** — the map from each in-flight blocking job to the waker of the task waiting on it. A job's whole life is: inserted by `BlockingTask::poll` when the awaiting task first polls, refreshed by that same poll on every later re-poll, read by `wake_completed_blocking` when the job finishes, and removed by `BlockingTask::poll` on completion and by `BlockingTask::Drop` on cancellation. This map feeds the termination check directly: `is_done` requires it to be empty, because a non-empty map means some task is still waiting on a job, and `run()` is not finished while any task waits.

**`next_blocking_id: BlockingId`** — the blocking id counter. `spawn_blocking` in `src/task/blocking.rs` reads it and increments it the same way `spawn_root` does with `next_id`. It is a separate counter because blocking ids are a separate identity space: allocating a blocking id must not disturb the task id sequence, and the two types cannot be mixed by the compiler.

### Construction

`RuntimeState::new` in `src/runtime_state.rs`:

```rust
impl RuntimeState {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(RuntimeState {
            queue: Arc::new(Mutex::new(Vec::new())),
            tasks: HashMap::new(),
            next_id: TaskId(0),
            blocking_wakers: HashMap::new(),
            next_blocking_id: BlockingId(0),
        }))
    }
}
```

The return type is `Rc<RefCell<Self>>`, not `Self`, and that is deliberate: every user of the state needs shared ownership of the *same* instance. The executor owns it, the `ContextHandle` carries a clone, and the queue's `Arc` is handed to every waker. Because every user needs the wrapper, the wrapper is built at the factory and no caller ever constructs a bare `RuntimeState`. `new` cannot fail: Rust does not expose allocation failure, so there is no `Result`; the only failures the state can produce are `RefCell` borrow panics at use time. `RuntimeState` has no `Drop` impl and no shutdown step of its own — it is created in `Mar::new` and dropped when the `Mar` drops, and its life is exactly the life of one `run()` call. The `Arc` queue can outlive it if a stale waker still holds a reference, which is safe: the wake pushes into a queue that is simply freed once the last waker drops (the test `waker_keeps_queue_alive` in `src/waker.rs` demonstrates this).

### Borrow Discipline

`RefCell` allows only one active borrow, shared or mutable, at a time. The invariant that keeps the runtime from panicking is that every borrow is scoped to the smallest block that needs it, so that a poll — which runs arbitrary user code — never happens while a borrow is held. `drain_ready_queue` in `src/mar.rs` is the example. First the pop:

```rust
let next = {
    let state = runtime.state.borrow_mut();
    let mut queue = state.queue.lock().unwrap();
    if queue.is_empty() {
        None
    } else {
        Some(queue.remove(0))
    }
};
```

The `{ }` block takes the borrow, pops an id, and ends, releasing the borrow before anything else happens. Then the task is removed with a borrow scoped to a single statement:

```rust
let Some(mut task) = runtime.state.borrow_mut().tasks.remove(&id) else {
    continue;
};
```

Then the poll runs with no borrow held at all:

```rust
match task.poll(&mut cx) {
    Poll::Pending => {
        runtime.state.borrow_mut().tasks.insert(id, task);
    }
    Poll::Ready(()) => {}
}
```

The poll's user code may call `spawn_root`, `spawn_blocking`, or `sleep`, and the first two borrow the state mutably. If the drain held a borrow across the poll, that user code would hit a second `borrow_mut()` on the already-borrowed `RefCell` and panic. The scoped blocks are the defense. The same discipline appears elsewhere: `spawn_blocking` allocates its id inside a `{ }` block and releases the borrow before returning, and `wake_completed_blocking` clones the waker out of the map inside a `{ }` block and releases the borrow before calling `wake_by_ref()`.

### Worked Example

Trace `Mar::run(async { let _ = crate::task::spawn_blocking(|| 21 * 2).await; })` and list all five fields at every step. Initial state after `Mar::new()`: queue `[]`, tasks `{}`, `next_id = TaskId(0)`, `next_blocking_id = BlockingId(0)`, `blocking_wakers = {}`.

1. `spawn_root(&runtime, future)` runs. `let id = state.next_id` reads `TaskId(0)`; `state.next_id.0 += 1` makes the counter `TaskId(1)`. The task is inserted and its id pushed. State: queue `[TaskId(0)]`, tasks `{TaskId(0): root future}`, `next_id = TaskId(1)`, `next_blocking_id = BlockingId(0)`, `blocking_wakers = {}`.

2. First loop iteration. `drain_ready_queue` pops `TaskId(0)` (queue `[]`), removes the task (tasks `{}`), and polls the root future. The root future's first poll reaches `spawn_blocking`. It reads the blocking counter:

```rust
let task_id = {
    let mut state = state.borrow_mut();
    let id = state.next_blocking_id;
    state.next_blocking_id.0 += 1;
    id
};
```

`BlockingId(0)` is taken and the counter becomes `BlockingId(1)`. The job is sent to the worker pool. The `.await` polls the returned `BlockingTask`, which inserts its waker:

```rust
this.state
    .borrow_mut()
    .blocking_wakers
    .entry(this.id)
    .and_modify(|existing| existing.clone_from(cx.waker()))
    .or_insert_with(|| cx.waker().clone());
```

and returns `Pending` because `try_recv()` found nothing yet. The root future returns `Pending`; the drain re-inserts the task. State: queue `[]`, tasks `{TaskId(0)}`, `next_id = TaskId(1)`, `next_blocking_id = BlockingId(1)`, `blocking_wakers = {BlockingId(0): waker for TaskId(0)}`.

3. `is_done` runs. tasks is not empty, so it is false. `poll_readiness_events` computes the timeout: the timer registry is empty, so `next_deadline()` is `None` and the executor parks forever, blocked inside the OS poller until something wakes it.

4. A worker thread receives the job, runs `21 * 2 = 42` inside `catch_unwind`, sends `Ok(42)` on the job's private result channel, sends `BlockingId(0)` on the completed channel, and calls `wake()` on the shared `mio::Waker`. The kernel wakes the parked executor; its poll returns with the pool's token set.

5. `fire_due_timers` runs (no timers, a no-op). Then `wake_completed_blocking`:

```rust
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
```

`drain_completed()` returns `[BlockingId(0)]`. The lookup clones the waker and `wake_by_ref()` pushes `TaskId(0)` onto the queue. State: queue `[TaskId(0)]`, `blocking_wakers` still `{BlockingId(0): waker}` — waking does not remove the entry; removal is the awaiting task's job.

6. Second loop iteration. `drain_ready_queue` pops `TaskId(0)` and polls the root future again. The `BlockingTask` poll refreshes its waker entry and then `try_recv()` returns `Ok(Ok(42))`. It runs the completion arm:

```rust
Ok(Ok(result)) => {
    this.done = true;
    this.state.borrow_mut().blocking_wakers.remove(&this.id);
    Poll::Ready(result)
}
```

`BlockingId(0)` is removed. The root future resumes past the `.await`, `let _ = 42`, and returns `Ready(())`. The task drops. State: queue `[]`, tasks `{}`, `next_id = TaskId(1)`, `next_blocking_id = BlockingId(1)`, `blocking_wakers = {}`.

7. `is_done` now sees tasks empty, the timer registry empty, and `blocking_wakers` empty, so `Mar::run` returns `Ok(())`.

Notice the two id spaces working independently: the task stayed `TaskId(0)` the whole time while the blocking id advanced from `BlockingId(0)` to `BlockingId(1)`. And notice that the `blocking_wakers` entry had two legitimate owners of its removal — `wake_completed_blocking` read it, but only the awaiting task (or its `Drop`) removes it.

### Interactions

The state is written by `spawn_root` and `drain_ready_queue` (in `src/mar.rs`), and by `spawn_blocking` and `BlockingTask::poll` (in `src/task/blocking.rs`). It is read by `drain_ready_queue`, `is_done`, and `wake_completed_blocking`. The ready queue is additionally written by `TaskWaker` (in `src/waker.rs`), which holds the queue but not the whole state — this is the one case where a component sees a single field rather than the whole struct. The `ContextHandle` (in `src/context.rs`) carries a clone of the state so leaf futures like `spawn_blocking` can reach it through the thread-local context. Every interaction is into the state; the state calls out to nothing.

### Source Links

- `src/runtime_state.rs` — the struct, both id types, `new`
- `src/mar.rs` — `spawn_root`, `drain_ready_queue`, `is_done`, `wake_completed_blocking`, and the state-shape tests
- `src/task/blocking.rs` — `spawn_blocking`, `BlockingTask::poll`, `BlockingTask::Drop`
- `src/waker.rs` — `TaskWaker`, the queue's other writer

## 3. Design Decisions and Tradeoffs

**Why `Rc<RefCell<RuntimeState>>` and not `Arc<Mutex<...>>`.** The decision: the whole runtime is single-threaded, so the state is shared through `Rc<RefCell<...>>`. The alternative is `Arc<Mutex<RuntimeState>>`. In this runtime the alternative is worse twice over. First, it costs nothing but overhead: atomic refcount increments and a lock on every access, on a thread that never contends. Second, it hides bugs: a re-entrant borrow — a second `borrow_mut()` while one is held — makes a `RefCell` panic immediately at the violating line with a message, while a `Mutex` would deadlock silently, hanging `run()` with no message at all. `Rc` also refuses to be sent across threads, so the single-threaded invariant is enforced by the compiler. The one field that still uses `Arc<Mutex<...>>` is the queue, and that is not a choice: the `Wake` contract requires `Waker::from(Arc<T>)` with `T: Send + Sync`, so the queue must live behind an `Arc` and a `Mutex` even though its containing state is `Rc<RefCell<...>>`. This would change if the executor became multi-threaded: the state would move behind an `Arc`, every field would need real synchronization, and the loud `RefCell` panic would become a deadlock-prone lock.

**Why separate `next_id` and `next_blocking_id`.** The decision: two independent counters of two distinct types. The alternative is one shared counter, or one shared id type. The alternative is worse because the two id spaces name different universes — tasks versus in-flight blocking jobs — and nothing requires them to move in lockstep: spawning a task must not consume a blocking id. The distinct newtypes are the point: `BlockingId` and `TaskId` are different types, so the compiler refuses to store a blocking id in the task table or pass a task id where a blocking waker lookup expects one. A single `usize` counter would lose that safety and make every mixed use a runtime bug instead of a compile error. The choice would change if the two identities were deliberately unified under one handle type, but that would trade compile-time safety for nothing.

**Why the ready queue lives on `RuntimeState` rather than directly on `Mar`.** The decision: the queue is a field of `RuntimeState`, not of `Mar`. The alternative — the queue as a field of `Mar` — would work for `spawn_root`, which takes `&Mar`. But the queue must be shared with every waker as an `Arc` for the waker's whole lifetime, and `spawn_root` needs the queue, the table, and the id counter together to build a task. Centralizing all scheduling state in one struct keeps a single source of truth: the `ContextHandle` carries one pointer, `state`, and leaf futures reach the blocking machinery through it. If the queue lived on `Mar`, the context would need an extra field, and the line between "scheduling state" and "the executor that happens to own it" would blur. The choice would change if the queue's ownership needed to differ from the rest of the state, but in this runtime every scheduling component wants the same one struct.

**When this design changes.** A multi-threaded executor swaps `Rc<RefCell<RuntimeState>>` for an `Arc` with internal locks, turns the ready queue into a concurrent structure (a per-thread queue or a work-stealing deque), and may replace dense task ids with slab indices. The single-threaded `RefCell` borrow discipline is possible precisely because one thread owns the state; the moment a second thread schedules, every field must synchronize independently.

## 4. Failure Modes and Misconceptions

### What Breaks If Implemented Wrong

**The queue and the table disagree.** An id sits in the queue but its task is already gone — a stale waker fired after its task completed. The drain's `let Some(mut task) = ... else { continue; }` handles this by design: the lookup fails and the id is skipped. If the drain instead panicked on a missing task, a stale timer would crash the whole runtime; the skip is what makes a late wake harmless. A disagreement in the other direction — a live task with no id ever queued — means the task is never polled again, and `run()` hangs.

**A `blocking_wakers` entry is never removed.** `is_done` requires the map to be empty, so a single stale entry makes `run()` never return. Two code paths must remove entries for the invariant to hold: `BlockingTask::poll` on completion (both the `Ok` value path and the `Err` panic-resume path call `blocking_wakers.remove`) and `BlockingTask::Drop` when the task is cancelled. The test `dropped_spawn_blocking_leaves_blocking_map_empty` in `src/mar.rs` drops a `BlockingTask` before completion and asserts the map is empty — if `Drop` were deleted, the runtime would hang forever.

**Ids are reused.** A reused task id turns a late, stale wake into a wrong poll: the executor would look up the new task and poll the wrong future. The monotonic counter prevents this, and there is no other mechanism — no check at the table can tell a stale wake from a fresh one, because both carry the same id.

**A borrow held across user code.** If `drain_ready_queue` held `borrow_mut()` while polling, a task that called `spawn_blocking` during its poll would trigger a second `borrow_mut()` and panic with a `RefCell` borrow error. The scoped `{ }` blocks that release the borrow before the poll are what prevent this.

### Common Misunderstandings

**"The ready queue holds the tasks."** It holds ids — one word each. The tasks live in the table, and the queue is only "who needs polling now". If the queue held tasks, it would duplicate futures, which a task cannot do.

**"The mutex protects against races."** There are no races; the runtime is single-threaded, and the lock is never contended. The `Mutex` exists only to make the `Vec` `Sync`, which the `Wake` contract requires for `Waker::from(Arc<T>)`.

**"A `Pending` task is lost."** A `Pending` task is parked: present in the task table, absent from the queue, waiting for its waker to push its id back. Losing a task is what happens when a future returns `Pending` without storing a waker — the parking structure is not the bug.

**"Ids can be reused after a task completes."** They are never reused. A stale waker can fire long after its task is gone, and monotonic ids guarantee that firing cannot land on a different task.

## 5. Summary

- `RuntimeState` is the single scheduling structure: the ready queue, the task table, the task id counter, the blocking-waker map, and the blocking id counter.
- It exists so every component that schedules — the executor, the wakers, `spawn_blocking` — sees the same data, and so a wake can be resolved to exactly the right task by id.
- It depends on `Task`, `TaskId`, `BlockingId`, and the waker types; it is carried by the `ContextHandle` so leaf futures can reach it.
- `spawn_root` and `drain_ready_queue` write the task side; `spawn_blocking` and `BlockingTask` write the blocking side; `is_done` and `wake_completed_blocking` read it, and its emptiness is what tells `run()` to return.
