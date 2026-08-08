# Tasks: The Container That Holds a Future

## What Problem Does a Task Solve?

An `async` block has been written, and the compiler has done its job. Behind the scenes it produced a state machine — a struct whose fields represent every variable that lives across an `.await` point, with a generated `poll` method that advances through the block one yield point at a time. But producing the state machine is only half the story. That struct cannot simply be pushed onto a queue and polled in a loop, because three hard constraints stand in the way.

The first constraint is that the type cannot be named. Every `async` block in Rust compiles to its own anonymous struct with a unique, unspellable name. Two different `async fn` bodies produce two completely different types that share no common ground except that both implement `Future`. An executor's queue of "things to poll" has to be a collection with a single element type — a vector of one concrete thing — and when every future has a different concrete type, that collection can't exist without some kind of indirection that erases the differences.

The second constraint is that the state machine is fragile in ways a normal struct is not. The compiler is free to generate self-referential fields inside an `async fn`: one field of the state machine can hold a pointer into another field of the same struct. This happens naturally when a local variable holds a reference to another local that lives across an `.await`. If the struct is ever moved in memory — shifted from one stack location to another, or from the stack to the heap and back — those internal pointers become wild, pointing at memory that might now contain anything. Rust's answer to this is `Pin`, a type that says "I promise this value will never move again." But `Pin` can only make that promise if the value has a stable address to begin with — it needs *ground* to anchor itself to.

The third constraint is that futures have no predictable size. One `async fn` might capture nothing but an integer and a bool; another might capture a vector of network buffers spanning megabytes. If the executor's job queue used fixed-size slots, it would either waste enormous memory making every slot big enough for the largest possible future, or overflow when a too-large future tried to enter. No fixed-size slot is right for every future.

These three constraints — unnameable types, self-referential fragility, and unknown size — lock together into a single conclusion: every async runtime must wrap futures in some container that solves all three at once. That container is the task.

## The Task Struct

Here's what this runtime's task looks like:

```rust
pub struct Task {
    id: TaskId,
    waker: Waker,
    future: Pin<Box<dyn Future<Output = ()>>>,
}
```

Three fields. Let's unpack them from the inside out.

### The Future: `Pin<Box<dyn Future<Output = ()>>>`

This is a mouthful. Let's break it down layer by layer.

**`Box`** puts the future on the heap. This solves the size problem — the `Task` struct itself is always the same small size (five machine words), and the variable-sized future lives somewhere else. It also provides a stable address, which `Pin` needs.

**`Pin`** is the no-move guarantee. Once the future is inside a `Pin<Box<...>>`, the compiler prevents it from being moved. Even though the `Task` struct itself might get moved in and out of hash maps during scheduling, the future stays at its heap address. `Pin` says: "this pointer can be dereferenced, but the thing it points to will never change address."

Think of it like a framed photograph. The `Task` is the frame — it can be picked up and moved around. The `Box` is the wall where the photo hangs — a fixed location. `Pin` is the nail holding the frame to the wall — it says "this stays put."

**`dyn Future`** is type erasure. It's a *trait object*: a fat pointer consisting of a data pointer (to the heap-allocated future) and a vtable pointer (to the functions that implement `Future` for whatever concrete type is behind the pointer). One `Task` type, any future type. The executor calls `poll` through the vtable and never needs to know what's on the other side.

**`Output = ()`** means the future produces no value when it finishes. Mar's executor discards results. If code needs to communicate a value back, it goes through channels or shared state — the task itself just says "I'm done."

### The ID: `TaskId(usize)`

A `TaskId` is a newtype wrapper around a plain `usize`. It's the runtime's internal name for this task. Every task gets a unique, never-reused id when it's spawned. The id appears in three places:

- **The task table** uses it as a key: `HashMap<TaskId, Task>`. When the executor needs to poll task #3, it looks up `TaskId(3)` in this map.
- **The ready queue** stores ids, not tasks: `Vec<TaskId>`. "Who needs polling?" is a much smaller question than "what are you and everything you contain?"
- **The waker** carries the id inside it. When a waker fires, it pushes its task's id onto the ready queue. That's the entire mechanism: a waker says "poll task #3" by enqueueing `TaskId(3)`.

Why a separate type instead of just `usize`? Because the compiler catches mistakes. A `BlockingId` is also `usize` under the hood, but it's a different type. Passing a `BlockingId` where a `TaskId` is expected — or pushing a `TaskId` into the blocking-waker map — becomes a compile-time error. The type system provides protection.

### The Waker

Every task owns its canonical waker. It's built when the task is spawned and lives as long as the task does. Each time the executor polls the task, it clones this waker to build the `Context`. Leaf futures — like `Sleep` or `BlockingTask` — clone it out of the `Context` and hand it to whatever external driver will eventually wake them (the timer registry, the worker pool's completion path).

Why does the task store a waker instead of building a fresh one each poll? Two reasons. First, identity: the same waker must be shared with all the leaf futures so they all wake the same task. Second, performance: cloning a pre-built waker is cheaper than constructing a new one.

## Building a Task

```rust
pub fn new(id: TaskId, f: impl Future<Output = ()> + 'static, waker: Waker) -> Self {
    let future = Box::pin(f);
    Task { id, waker, future }
}
```

`Box::pin(f)` does two things at once: it allocates `f` on the heap *and* wraps it in `Pin`. The type parameter accepts any future at the call site, but `Task` itself stays generic-free — the type erasure happens when the field coerces to `dyn Future`.

## The Life of a Task

A task goes through four phases:

### Phase 1: Spawning

`spawn_root` in `src/mar.rs` does the setup:

```rust
let mut state = runtime.state.borrow_mut();
let id = state.next_id;
state.next_id.0 += 1;
let waker = TaskWaker::new(state.queue.clone(), id);
state.tasks.insert(id, Task::new(id, future, waker));
state.queue.lock().unwrap().push(id);
```

Read the next id, bump the counter. Build the waker from the shared ready queue and the id. Insert the task. Push the id onto the queue. The future is now in the system.

### Phase 2: Polling

`drain_ready_queue` picks up the id, removes the task from the table, and polls it:

```rust
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
```

Notice the remove-before-poll pattern. The task is taken *out* of the table, polled, then either re-inserted (if it returned `Pending`) or dropped (if it returned `Ready`). This means no task is ever in the table while being polled. It's a clean invariant: a task is either idle in the table, or actively being polled with the table not responsible for it.

A `Pending` task is *parked*. It's in the table but not in the queue. It's alive, its progress is saved, but it won't run again until something — a timer, a worker thread, an I/O event — calls `wake()` on its waker.

### Phase 3: Waking

A waker fires. It pushes this task's id onto the ready queue. The next drain pops it and polls again. That's it — one line of code in the waker's implementation:

```rust
self.queue.lock().unwrap().push(self.id);
```

No callback, no direct call into the executor, no polling from inside the waker. Just a push. The executor picks it up on the next loop iteration.

### Phase 4: Completion

When `poll` returns `Ready(())`, the task is dropped. The `Box` deallocates the future's heap memory. The waker's reference count drops. The map entry is gone. The runtime notices in `is_done` when the table is empty.

## Walking Through a Self-Waking Task

The `Probe` test future in `src/mar.rs` is the simplest task that exercises every phase:

```rust
impl Future for Probe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        if self.polls.get() >= self.target {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
```

`Probe` counts polls. It returns `Pending` until it's been polled `target` times, and on each `Pending` it calls `wake_by_ref()` to re-schedule itself immediately.

Trace a `Probe { target: 3 }` through Mar:

1. **Spawn.** Task gets `TaskId(0)`, inserted, id pushed. Queue: `[TaskId(0)]`, table: `{TaskId(0): Probe(polls=0, target=3)}`
2. **First poll.** Queue pops `TaskId(0)`. Task removed from table. `Probe::poll`: polls becomes 1, 1 < 3, calls `wake_by_ref()` which pushes `TaskId(0)` back, returns `Pending`. Task re-inserted. Queue: `[TaskId(0)]`, table: `{TaskId(0)}`.
3. **Second poll.** Same thing: polls becomes 2, still < 3, wake, re-insert. Queue: `[TaskId(0)]`, table: `{TaskId(0)}`.
4. **Third poll.** Polls becomes 3, >= target, returns `Ready(())`. Task dropped. Queue: `[]`, table: `{}`.
5. **Drain loop ends.** Next check finds empty queue, breaks. `is_done` sees empty table. `run()` returns.

Notice that between polls 1 and 2, the `Task` struct itself moved — it was removed from the table, had `poll` called on it, and re-inserted into the table at a potentially different memory location. But the `Probe` future *inside* the `Box` never moved. It stayed at its fixed heap address, which is why the `polls` counter survived across polls. The `Pin<Box<...>>` combination is what makes this work.

## Design Choices Worth Understanding

**Why `Box`?** Could a fixed-size arena work? An arena requires knowing the maximum future size in advance, but that size is a compiler secret. Either enormous amounts of memory would be wasted (making every slot big enough for the worst case) or overflow would occur. `Box` gives each future exactly the space it needs.

**Why `Output = ()`?** The executor's job is completion, not value extraction. `Mar::run` takes `Future<Output = ()>` because the executor doesn't care what the code produced — it only cares that it finished. If typed results are needed, they are communicated through channels (which is exactly how `spawn_blocking` works).

**Why `HashMap` and not a `Vec` of slots?** Tasks complete in arbitrary order, not FIFO. A `HashMap` gives O(1) insert, lookup, and remove regardless of what ids are still alive. A slab allocator (dense array of slots with free lists) would also work — tokio does exactly that — but a `HashMap` keeps the implementation simple and readable.

## Common Misconceptions

**"`Pending` means the future is broken."** A `Pending` future is parked, not broken. It's alive and waiting in the task table. It'll run again when its waker fires.

**"`Pin` and `Box` are the same thing."** They solve different problems. `Box` puts data on the heap and gives a stable address. `Pin` forbids moving the data. A future needs both: heap allocation so its address is stable, and pinning so that address is guaranteed to never change.

**"The executor polls tasks on a timer."** The executor polls only what's in the ready queue. A task gets polled when its waker pushes its id onto that queue — and that waker is held by whatever external thing the task is waiting for. It's event-driven, not timer-driven.

## Summary

A `Task` is the runtime's unit of work: an id for identity, a waker for wake-ups, and a `Pin<Box<dyn Future<Output = ()>>>` for holding the actual async work. The `Pin<Box<...>>` combination solves the three hard problems of async Rust — unnameable types, self-referential futures, and unknown sizes — allowing the executor to treat every future uniformly through one container type.

Source: `src/task/mod.rs`
