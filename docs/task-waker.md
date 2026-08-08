# The Task Waker: Re-Queuing Tasks When Something Happens

## 1. Concept: What Is a Waker?

### The Core Concept

A waker is a cloneable, thread-safe handle that, when `wake()` is called on it, asks the executor to re-poll one specific task. It is the only mechanism by which a task that has returned `Pending` is ever run again.

A `Future` is a value advanced by a `poll` method the executor calls repeatedly, each call returning `Ready(output)` ("done") or `Pending` ("not done; no progress is possible right now"). `Pending` means the future waits for something outside it — a byte on a socket, a deadline, a worker thread's result. The runtime cannot just re-poll: it does not know when the waited-for thing will happen, and polling with nothing new to do is wasted work. The future cannot resume itself: nothing inside it runs between polls. So the thing it waits for must signal the runtime "poll this task again". That signal is the waker.

The standard library provides the type. `std::task::Waker` is an opaque, cloneable handle: a *fat pointer* — a data pointer to a reference-counted payload plus a vtable pointer to the functions for cloning, waking, and dropping it. It is `Send + Sync`. A runtime implements the safe `std::task::Wake` trait instead of touching the internals:

```rust
pub trait Wake {
    fn wake(self: Arc<Self>);
    fn wake_by_ref(self: &Arc<Self>) { ... }
}
```

`wake` consumes an `Arc` of the payload; `wake_by_ref` borrows it, so one stored waker can fire several times (its default clones the `Arc` and calls `wake`). The conversion `Waker::from(Arc<T>)`, where `T: Wake + Send + Sync + 'static`, builds the vtable, clone, `wake_by_ref`, and reference-counted drop for you. No `unsafe`, no hand-rolled vtable.

The contract: `wake()` means "this task may make progress now; arrange for it to be polled". It does **not** run the task's code. If a task returned `Pending`, at least one `wake` will eventually fire, and after it the task is eligible for re-polling. Two parties are involved, and no component is both: the **executor** creates wakers when it spawns tasks; external **drivers** (the timer registry, the blocking-pool completion path) call `wake`/`wake_by_ref` on wakers they were handed; the executor responds by polling. A waker never polls anything.

### The Problem

Here is the naive way to drive a future that may return `Pending`:

```rust
use std::future::Future;
use std::task::{Context, Poll};

let mut fut = Box::pin(read_socket());   // returns Pending until a byte arrives
let mut cx = Context::from_waker(&waker); // waker: a std::task::Waker

let value = loop {
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => break v,
        Poll::Pending => {
            // Wrong: re-poll immediately, in a hot loop.
        }
    }
};
```

This loop is a busy wait: on `Pending` it re-polls immediately, nothing waiting for the socket. A core spins at 100%, and the future's `poll` body runs thousands of times per second with no change. Sleeping between polls is no better — a fixed `thread::sleep(10ms)` adds up to 10ms of latency and still re-polls a hundred times a second. In an async runtime the failure is worse than wasted CPU: a spinning or sleeping loop blocks every other task, every timer, and all I/O. The loop cannot win, because it has no way to be *told* when the socket is ready; it can only guess, by polling.

The waker removes the guessing. The future hands its waker to the thing it waits for; when the byte arrives, that thing calls `wake()`; the executor polls the task once and moves on. One poll per state change instead of thousands.

### Why Any Runtime Needs This

Every runtime faces the same two facts: a `Pending` future cannot resume itself, and the thing it waits for lives outside the runtime. Every runtime needs a wake channel, and every one uses the same standard-library type with the same contract. Only the destination differs: tokio pushes the task's id onto a worker-local or global run queue; smol pushes onto a thread-local queue; this runtime pushes onto one shared `Arc<Mutex<Vec<TaskId>>>`. The `Waker` type and `Wake` trait keep that interface uniform.

## 2. How This Runtime Implements TaskWaker

### The Struct

The runtime's waker is `TaskWaker`, in `src/waker.rs`:

```rust
pub struct TaskWaker {
    queue: Arc<Mutex<Vec<TaskId>>>,
    id: TaskId,
}
```

### Field by Field

**`queue: Arc<Mutex<Vec<TaskId>>>`.** A reference-counted handle to the shared ready queue. `Arc` is the standard-library atomically reference-counted pointer, for shared ownership across clones. `Mutex` is the standard mutual-exclusion wrapper: it grants interior mutability to the `Vec` and makes the whole value `Sync`, safe to share across threads. `TaskId` is a newtype over `usize` defined in `src/runtime_state.rs`:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TaskId(pub usize);
```

The field is written at construction — `spawn_root` passes `state.queue.clone()`, a second reference to the runtime's single ready queue — and read by `wake`, which pushes onto it. The waker stores the queue itself rather than a channel, callback, or executor handle, because a `wake` must be exactly "push my id into the shared queue". The `Arc` lets every waker hold the same queue without a global; when the runtime and all wakers are gone, the count reaches zero and the queue is freed.

**`id: TaskId`.** Which task this waker re-queues. Written at construction, read by `wake`. It is the waker's entire identity: no reference to a task or future, only the id the executor uses to look the task up in its table. Two wakers differ only in this field.

### Construction

```rust
impl TaskWaker {
    pub fn new(queue: Arc<Mutex<Vec<TaskId>>>, id: TaskId) -> Waker {
        Waker::from(Arc::new(TaskWaker { queue, id }))
    }
}
```

Two steps. `Arc::new(TaskWaker { queue, id })` allocates the payload on the heap with a reference count of one; this is the object the generated vtable will manage. `Waker::from` wraps it: std sees that `TaskWaker: Wake + Send + Sync + 'static`, builds the vtable, and returns an opaque `Waker`. The caller never touches the payload again.

### Core Methods

The only method that matters is the `Wake` impl:

```rust
impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.lock().unwrap().push(self.id);
    }
}
```

`self.queue.lock()` acquires the queue's `Mutex`, returning a guard that dereferences to `&mut Vec<TaskId>`; `.unwrap()` asserts the lock was not poisoned; `.push(self.id)` appends the id. The whole action is one lock-push: no deduplication, no existence check, no call into the executor — a pure signal. `wake_by_ref` is the trait's default, which clones the `Arc` and calls `wake`, the same lock-push.

### Lifecycle

A waker's life has six phases.

1. **Creation.** `spawn_root` in `src/mar.rs` reads the next task id, calls `TaskWaker::new(state.queue.clone(), id)`, and stores the result on the `Task`.
2. **Storage.** The `Task` keeps the canonical `Waker` in its `waker` field.
3. **Use during poll.** `drain_ready_queue` clones it to build the `Context`: `let waker = task.waker().clone(); let mut cx = Context::from_waker(&waker);`.
4. **Propagation.** A leaf future — a `Sleep`, a `BlockingTask` — clones it out of `cx` and stores it where a driver can reach it: the timer registry's entry list or the `blocking_wakers` map.
5. **Firing.** The external condition happens; the driver calls `wake_by_ref()`; the task's id is pushed onto the shared queue.
6. **Response.** The executor's next drain pops the id and polls the task. The waker's refcount falls as its stored clones are removed and the `Task` drops.

### Worked Example

Trace a run that exercises every phase: `Mar::run(async { task::spawn_blocking(|| 21 * 2).await; })`. The root task is `TaskId(0)`. Start state: queue `[]`, table `{}`, `next_id = TaskId(0)`, `next_blocking_id = BlockingId(0)`, `blocking_wakers = {}`.

1. `spawn_root` runs. `id = TaskId(0)`; `next_id` becomes `TaskId(1)`. The `TaskId(0)` waker is built by `TaskWaker::new(state.queue.clone(), TaskId(0))` and stored on the task. Queue `[TaskId(0)]`, table `{TaskId(0)}`.
2. `drain_ready_queue` pops `TaskId(0)` (queue `[]`), removes the task (table `{}`), clones its waker, and polls. Inside the root future, `spawn_blocking` reads the context, allocates `BlockingId(0)` (`next_blocking_id` becomes `BlockingId(1)`), submits the job, and creates a `BlockingTask`. `BlockingTask::poll` runs with the same `cx`; it inserts the `TaskId(0)` waker into `blocking_wakers` under `BlockingId(0)` and, finding no result, returns `Pending`. The root future returns `Pending`; the task is re-inserted. Queue `[]`, table `{TaskId(0)}`, `blocking_wakers = {BlockingId(0): Waker(TaskId(0))}`.
3. `is_done` is false (table non-empty). The executor parks with no timeout (the timer registry is empty) and blocks in `mio::Poll::poll`.
4. A worker thread runs `21 * 2 = 42`, sends `Ok(42)` on the result channel and `BlockingId(0)` on the completed channel, then calls `wake()` on the pool's `mio::Waker`. The kernel unblocks the parked poll with token `WAKEN_TOKEN`.
5. `wake_completed_blocking` sees the token, drains the completed channel to `[BlockingId(0)]`, looks up `BlockingId(0)` in `blocking_wakers`, and calls `wake_by_ref()`. The `Wake` impl's lock-push runs: queue becomes `[TaskId(0)]`.
6. The next drain pops `TaskId(0)`, removes the task, and polls. `BlockingTask::poll` refreshes the waker entry, then `try_recv` returns `Ok(Ok(42))`: it removes the `BlockingId(0)` entry, marks itself done, and returns `Ready(42)`. The root future completes; the task drops. Table `{}`, queue `[]`, `blocking_wakers = {}`.
7. `is_done` is true; `run()` returns `Ok(())`.

The waker's path: created in step 1, cloned into the `Context` in step 2, stored by the pool path in step 2, fired in step 5 with a one-line lock-push, answered by the executor in step 6. The timer path is the same shape with a different driver: `Sleep::poll` stores the waker in the timer registry, and `expire_due` fires it with `wake_by_ref()` when the deadline passes. Even a self-waking future — the `Probe` test in `src/mar.rs` — uses the same queue: its poll calls `cx.waker().wake_by_ref()`, and the id lands mid-drain.

### Interactions

`spawn_root` (`src/mar.rs`) creates every `TaskWaker`. `drain_ready_queue` (`src/mar.rs`) clones the task's waker into a `Context` and responds to queued ids. Drivers consume wakers they were handed: `Sleep::poll` stores one in the timer registry (`src/time/sleep.rs`), `BlockingTask::poll` stores one in `blocking_wakers` (`src/task/blocking.rs`); `expire_due` (`src/time/timer_registry.rs`) and `wake_completed_blocking` (`src/mar.rs`) fire them. The waker calls into one thing only: the shared queue in `RuntimeState`. Every interaction is one-directional.

### Source Links

- `src/waker.rs` — `TaskWaker`, `TaskWaker::new`, the `Wake` impl, and the waker tests
- `src/mar.rs` — `spawn_root` (creates wakers), `drain_ready_queue` (responds to them), `wake_completed_blocking` (fires them)
- `src/runtime_state.rs` — `TaskId` and the shared queue inside `RuntimeState`
- `src/time/sleep.rs` and `src/time/timer_registry.rs` — one driver that stores and fires wakers
- `src/task/blocking.rs` — the other driver that stores and is fired

## 3. Design Decisions and Tradeoffs

**Why `Arc` + `Mutex` and not `Rc` + `RefCell`.** `Waker::from(Arc<T>)` requires `T: Send + Sync`. `Rc` implements neither, so `Rc<RefCell<Vec<TaskId>>>` would not compile — the program fails at build time. The waker must be `Send + Sync` because a stored waker may be moved to or called from another thread: a worker thread's completion path leads to the executor waking blocking tasks, and a timer-registry entry could be touched from anywhere. The `Mutex` satisfies the `Sync` bound, not because threads contend: the runtime is single-threaded, so `lock().unwrap()` is trivial. This would change if the queue were genuinely shared across concurrent threads.

**Why the waker enqueues instead of polling directly.** Two reasons. Separation of concerns: the waker is a pure signal and the executor owns the poll; a waker that polled would duplicate the executor's bookkeeping (remove, poll, re-insert) in a second place. Reentrancy: if `wake` polled directly, a wake could fire while the executor was mid-poll of another task, causing reentrant execution of the runtime loop. Enqueueing defers the poll to the top of the next drain iteration, where the executor is not inside a poll.

**Why `wake()` is a single lock-push.** Correctness requires only that the task be re-polled, not exactly once. Multiple `wake` calls produce multiple queue entries; a duplicate poll returns `Pending` again if the task is not ready, or finds it done and skips it. Deduplication would be an optimization, not needed for termination. The `Probe` test relies on this: it wakes itself on every `Pending`, and the drain re-polls until `Ready`.

**When this design changes.** A multi-threaded executor would not share one `Arc<Mutex<Vec<TaskId>>>`; each worker would push to its own local queue or work-stealing deque. The `Arc` payload is also a deliberate choice over `Weak`: a stale waker still holds the queue alive and still pushes, and the drain's `else { continue; }` skips the missing id. The `waker_keeps_queue_alive` test in `src/waker.rs` pins that behavior: dropping the runtime's reference to the queue does not invalidate a surviving waker. A `Weak`-based design would make a stale waker a silent no-op; this runtime chooses `Arc` because "push anyway, skip on drain" is simpler.

## 4. Failure Modes and Misconceptions

### What Breaks If Implemented Wrong

**A future returns `Pending` but never stores a waker.** No driver holds a waker, so no `wake` can arrive; the task parks in the table and is never re-queued; `run()` hangs forever. The waker is the only path back into the queue, so `Pending` without a stored waker is a permanent stall. Every future that returns `Pending` — `Sleep`, `BlockingTask` — stores its waker on its first poll.

**A waker's id no longer exists when it fires.** This is the designed spurious-wake path. The task completed and dropped, but a driver still held a clone of its waker. `wake` pushes the stale id; the drain's lookup fails — `let Some(mut task) = ... else { continue; }` — and the loop moves on. Nothing crashes and no wrong task is polled, because the drain verifies each id.

**`wake` polled directly instead of pushing.** A wake arriving during another task's poll would re-enter the poll path mid-drain while the `RuntimeState` borrow and queue lock are still held — a `RefCell` double-borrow panic. The lock-push defers polling to the next drain, where the executor is in a consistent state.

### Common Misunderstandings

**"`wake()` runs my task's code."** It runs one statement: `self.queue.lock().unwrap().push(self.id);`. The task's code runs later, when the drain polls it.

**"Wakers are callbacks."** A callback is a function the driver invokes. A `Waker` is a handle to a shared queue; calling it pushes an id into that queue. It never calls the executor or the future.

**"Waking twice is a bug."** Duplicate wakes are harmless: they produce duplicate queue entries, and the second poll finds the task already complete or still pending. Correctness needs at least one wake, not exactly one.

**"The waker knows the future."** It knows one thing: its task's `TaskId`. Everything else — locating the task, polling it, deciding it is done — is the executor's job. That is why a waker can fire after its task is gone.

## 5. Summary

- A waker is a cloneable, thread-safe handle that re-queues a specific task by pushing its `TaskId` into the shared ready queue.
- It exists because a `Pending` future cannot resume itself; an external driver must request the re-poll, and `wake` is that request.
- It depends on `TaskId` for identity, the shared `Arc<Mutex<Vec<TaskId>>>` queue for its destination, and std's `Wake` trait for the vtable, clone, and drop.
- Timer expiry, blocking completions, and self-waking futures all funnel through the same path: store a waker → fire it with `wake_by_ref()` → push the id → drain → poll.
