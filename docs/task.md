# Tasks: The Unit of Async Work

## 1. Concept: What Is a Task?

### The Core Concept

A task is one unit of async work owned by the runtime: a pinned, heap-allocated future plus the identity and waker that let the runtime schedule it. To understand why it has those exact parts, start with the trait that defines what "one unit of async work" means.

The `Future` trait is the core abstraction of async Rust. Here it is, from the standard library:

```rust
pub trait Future {
    type Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}

pub enum Poll<T> {
    Ready(T),
    Pending,
}
```

`type Output` is the value the future produces when it finishes. `self: Pin<&mut Self>` is a mutable reference to the future wrapped in `Pin`; the pin guarantees the future will not be moved in memory between polls. `cx: &mut Context<'_>` is a handle to the current poll's environment; its only useful content is a `Waker`. `poll` returns `Poll<Self::Output>`, which has two variants: `Ready(T)` means "done, here is the value", and `Pending` means "not done, poll me again later".

The contract around `poll` is strict. A future that returns `Ready(T)` is consumed; the caller must not poll it again. A future that returns `Pending` must have stored the `Waker` it received in `cx` somewhere reachable, so it can be woken when it can make progress. A future that returns `Pending` without arranging a wake will never be polled again, and the runtime will wait forever.

**The self-referential problem and `Pin`.** The compiler turns every `async fn` into a state machine struct. Each local variable that lives across an `.await` becomes a field of that struct, and the struct also stores the inner future being awaited. This works only if a field's address can stay stable: if one field holds a pointer into another field of the same struct, moving the struct invalidates that pointer. Here is the shape:

```rust
struct SelfReferential {
    data: String,
    data_ptr: *const String, // points at self.data
}
```

If a `SelfReferential` is moved to a new memory address, `data_ptr` still points at the old address, and the next read through it reads freed or reused memory. This is exactly what async state machines can contain. `Pin` is the fix: `Pin<P>` guarantees the value behind `P` cannot be moved once it is pinned, so `Pin<&mut F>` means "a mutable reference to an `F` that will not move". Because a polled future may contain self-pointers, the rule is: once polled, never move the future again. That is why `poll` takes `self: Pin<&mut Self>` rather than `self: &mut Self`: the type system enforces the no-move rule instead of relying on the runtime to remember it.

**Why heap-allocated.** There are two separate reasons, and both are necessary. (1) *Unknown size.* The executor cannot know how large any future is, so it cannot hold futures by value in fixed-size slots. Placing the future behind a `Box` moves the unknown size onto the heap and leaves every `Task` the same small, known size. (2) *Stable address.* The `Task` moves constantly — the executor removes it from the task map, polls it, re-inserts it. The future inside must not move with it, and the `Box` keeps it at a fixed heap address.

**Cooperative scheduling.** This runtime is cooperative: a task runs until its `poll` returns `Pending` or `Ready`. Nothing preempts it. An OS thread can be suspended by the kernel's scheduler at any instruction; a polled future cannot. A task that never returns `Pending` — always ready, or busy-looping — runs forever and starves every other task, because no runtime machinery can interrupt a poll in progress. Every single-threaded async executor works this way.

**What the runtime must do with a task.** A runtime has exactly four operations on a task, and each maps to a design element of this codebase: *store* it (in a task table keyed by id), *poll* it (when it is in the ready queue), *wake* it (via a waker that pushes its id back onto the queue), and *drop* it (when `poll` returns `Ready`). This article is about the container; the other operations live in the executor and the waker.

### The Problem

Now that the concept is established, here is a future that cannot be run by itself:

```rust
use std::future::Future;
use std::task::{Context, Poll};

// `waker` is a std::task::Waker; it is defined in the next section.
let mut cx = Context::from_waker(&waker);
let mut fut = async { /* work with several awaits */ };

match fut.poll(&mut cx) {
    Poll::Ready(value) => value,
    Poll::Pending => {
        // fut is a compiler-generated state machine of unknown size and
        // of a concrete type we cannot name. We cannot store it here next
        // to other futures, and if we drop it, every step of work it
        // already completed is lost forever. Nothing will ever poll it
        // again.
    }
}
```

This snippet fails in three distinct ways, and each failure is what the `Task` type exists to fix.

First, the type. An `async fn` or `async` block compiles to a unique anonymous struct that stores every local variable that lives across an `.await`. Two different `async` blocks have two different types, and the compiler will not let you name either. A runtime must hold many futures in one data structure — a queue, a map — and no concrete type can serve; the future must be *type-erased*, stored behind a `dyn Future` so the runtime handles it through one shared interface.

Second, the size. A future captures every value that crosses an await point; a small `async fn` may be a few dozen bytes, one holding a large buffer across an `.await` can be megabytes. The runtime cannot lay futures out by value in a fixed-size structure, because it does not know how big any given future is.

Third, the lifetime of work. A future that returns `Pending` is not finished. It has made progress and it expects to be polled again; dropping it discards that progress and leaves whatever it was waiting for pointing at a task that no longer exists.

The `Task` is the container that solves all three: a heap allocation (size), a trait object (type erasure), a pin (safe re-polling), and an id (identification).

### Why Any Runtime Needs This

Every async runtime must hold futures that return `Pending` and poll them repeatedly, and every one reaches for the same container shape. Tokio wraps each future in a heap-allocated, pinned `Task` identified by a `TaskId` and stored in a slab allocator; async-std and smol do the same. The details differ — tokio uses a slab of ids, this runtime uses a `HashMap` — but the four operations are identical: store, poll, wake, drop. A `Task` type is not an optimization; it is the minimal bookkeeping unit that makes "poll me again later" possible.

## 2. How This Runtime Implements Tasks

### The Struct

The `Task` lives in `src/task/mod.rs`:

```rust
pub struct Task {
    id: TaskId,
    waker: Waker,
    future: Pin<Box<dyn Future<Output = ()>>>,
}
```

### Field by Field

**`id: TaskId`.** `TaskId` is a newtype over `usize`, defined in `src/runtime_state.rs`:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TaskId(pub usize);
```

The newtype wraps a plain `usize` in a distinct type so it cannot be confused with other numbers. It has three roles. It is the key into the task table (`HashMap<TaskId, Task>`). It is the payload the waker pushes onto the ready queue. And it is the value embedded in the task's waker, so a wake can name exactly which task to re-poll. Without a stable id, a woken task could not be connected back to its future.

**`waker: Waker`.** This is the task's canonical waker. `Waker` is the standard-library handle that requests a re-poll: a cloneable reference to an opaque payload whose `wake()` method causes the runtime to schedule its task again. It is built by `TaskWaker::new`, this runtime's waker implementation, in `spawn_root`, and stored on the task. The executor reads it back in `drain_ready_queue` to build the `Context` it polls with. The task stores it rather than rebuilding it each poll for two reasons: the same waker identity must be shared with the future's children (leaf futures clone it out of the `Context` and hand it to timers and the blocking pool), and reusing one waker is cheaper than building a fresh one.

**`future: Pin<Box<dyn Future<Output = ()>>>`.** Unwrap the layers from the inside out. `Box` places the future on the heap, solving unknown size and giving a stable address. `Pin` extends the no-move guarantee through the indirection: `Pin<Box<F>>` pins the boxed pointee, so the future at the heap address cannot be moved. `dyn Future<Output = ()>` is a *trait object*, a *fat pointer*: two words — a data pointer to the actual future and a vtable pointer to the function table for its `Future` impl. The `dyn` keyword is the type erasure: one `Task` type can hold any future type, because the executor only ever talks to it through the vtable. `Output = ()` pins the output type: the executor only needs to know a task finished, and it discards the value. The implicit `'static` requirement holds because a future stored in the task table outlives any borrow from the scope that spawned it.

### Construction

`Task::new` in `src/task/mod.rs`:

```rust
pub fn new(id: TaskId, f: impl Future<Output = ()> + 'static, waker: Waker) -> Self {
    let future = Box::pin(f);
    Task { id, waker, future }
}
```

`Box::pin(f)` does two things in one call: it allocates `f` on the heap and it pins it, giving `Pin<Box<...>>` directly. `Box::pin` does not perform the type erasure — that happens when the value is coerced to `dyn Future` at the assignment to the field. The generic parameter `impl Future<Output = ()> + 'static` accepts any future at the call site; `Task` itself stays generic-free.

### Core Methods

The three methods on `Task` are thin:

```rust
pub fn id(&self) -> TaskId {
    self.id
}

pub fn waker(&self) -> &Waker {
    &self.waker
}

pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
    self.future.as_mut().poll(cx)
}
```

`id()` and `waker()` return what they store. `poll` forwards: `self.future` is a `Pin<Box<dyn Future>>`; `as_mut()` gives `Pin<&mut dyn Future>` (the pin is passed through the `Box`, not created here), and `.poll(cx)` is the dynamic dispatch through the vtable to the concrete future's `poll`. The `Poll<()>` return type matches the erased future's output type.

### Lifecycle

A task's life has four phases, each triggered by a different component.

**Creation.** `spawn_root` in `src/mar.rs` builds the task:

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

Line by line: borrow the shared `RuntimeState` mutably; read the current id `TaskId(0)`; increment the counter so ids are never reused; build the waker from the shared ready queue and the id; insert the new `Task` into the task map; push the id onto the ready queue. The future has entered the runtime.

**Polling.** `drain_ready_queue` in `src/mar.rs` pops ids and polls:

```rust
fn drain_ready_queue(runtime: &Mar) {
    loop {
        let next = {
            let state = runtime.state.borrow_mut();
            let mut queue = state.queue.lock().unwrap();
            if queue.is_empty() {
                None
            } else {
                Some(queue.remove(0))
            }
        };
        let Some(id) = next else {
            break;
        };

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

This is the remove-poll-reinsert dance. The task is removed from the map *before* polling, so the map never contains a task mid-poll. If `poll` returns `Ready(())`, the task is dropped at the end of the match arm and never re-inserted: it ceases to exist. If `poll` returns `Pending`, the task is re-inserted into the map under its id. The `else { continue; }` handles a stale id: if a waker pushed an id whose task is already gone, the lookup fails and the loop simply moves on.

A `Pending` task is *parked*: present in the task table, absent from the ready queue. It is not being polled, and it will not be polled again on its own; it waits in the table so that when its waker fires, the executor can look it up by id, re-queue it, and poll it. Parking is the resting state of cooperative scheduling.

**Waking.** The waker fires — pushed by a timer, the reactor, or the blocking pool — and pushes this task's id onto the shared ready queue. `drain_ready_queue` pops that id and polls again. This is the "who wakes whom" contract in full: the executor creates the waker in `spawn_root`; the future stores a clone of it with an external driver; the driver calls `wake()`; the executor responds by polling. The task itself never calls `poll` on itself.

**Teardown.** When `poll` returns `Ready(())`, the future's output value is discarded (`Output = ()`), the `Task` drops, the future drops and frees its heap allocation, and the map entry stays empty. The runtime notices in `is_done`, which returns `true` when the task table, the timer registry, and the blocking-waker map are all empty.

### Worked Example

Trace `Mar::run(Probe { target: 3, polls })`, where `Probe` is the test future in `src/mar.rs`:

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

`Probe` returns `Pending` until it has been polled three times; on each `Pending` it re-wakes itself by calling `wake_by_ref()` on the waker it received. Start state after `Mar::new()`: queue `[]`, tasks `{}`, `next_id = TaskId(0)`.

1. `spawn_root` runs. `id = TaskId(0)`, `next_id` becomes `TaskId(1)`. The waker for `TaskId(0)` is built. The task is inserted and its id pushed. State: queue `[TaskId(0)]`, tasks `{TaskId(0): Probe(target 3)}`.
2. `drain_ready_queue`, first iteration. It pops `TaskId(0)` (queue `[]`), removes the task (tasks `{}`), clones its waker, and polls. `Probe::poll` runs: `polls` becomes `1`; `1 < 3`, so it calls `wake_by_ref()`, pushing `TaskId(0)` back onto the queue, and returns `Pending`. The task is re-inserted. State: queue `[TaskId(0)]`, tasks `{TaskId(0)}`.
3. The drain repeats: pops `TaskId(0)`, polls. `polls` becomes `2`; still `< 3`; `wake_by_ref()` pushes the id again; returns `Pending`; re-inserted. State: queue `[TaskId(0)]`, tasks `{TaskId(0)}`.
4. The drain repeats: pops `TaskId(0)`, polls. `polls` becomes `3`; `3 >= 3`, so `Probe` returns `Ready(())`. The `Ready` arm drops the task; the table is `{}`. The next iteration finds the queue empty and breaks.
5. `is_done` checks: task table empty, timer registry empty, blocking-waker map empty — all three hold. `Mar::run` returns `Ok(())`.

The wake fired by the future itself (step 2) is what kept the drain alive: `wake_by_ref` pushed the id *during* the drain, so the next loop iteration found work. Between steps 2 and 3 the `Task` moved (out of the map and back in), but the `Probe` future did not: it stayed at its heap address inside the `Box`, which is why its `polls` counter survived.

### Interactions

The executor writes and reads the task: `spawn_root` (in `src/mar.rs`) creates it; `drain_ready_queue` polls it and drops it on `Ready`. `TaskWaker` (in `src/waker.rs`) supplies the `Waker` that the drain clones into a `Context`. `RuntimeState::tasks` (in `src/runtime_state.rs`) is where the task lives while parked. The task itself calls out to nothing; it is a passive container.

### Source Links

- `src/task/mod.rs` — the `Task` struct, `Task::new`, the accessors, and the size test
- `src/runtime_state.rs` — `TaskId` and the `RuntimeState` that holds the task table
- `src/mar.rs` — `spawn_root` and `drain_ready_queue`
- `src/waker.rs` — `TaskWaker`, which builds the task's waker

## 3. Design Decisions and Tradeoffs

**Why `Pin<Box<dyn Future>>` instead of a fixed-size arena or stack allocation.** The alternatives fail in this runtime. *Stack allocation:* an async future is a state machine that must survive the scope that spawned it and the executor's arbitrary scheduling order; a future on the executor's stack is gone when that stack frame returns. *A slab or arena keyed by index:* such a structure must know the size of what it stores, but each future's size is a compiler secret; an arena would force every future into one fixed slot size, wasting memory on large futures. The combination `Pin<Box<dyn Future>>` gives a stable heap address (Box), the no-move guarantee (Pin), type erasure (dyn), and a constant `Task` size (Box). This choice would change if the runtime targeted a fixed, known set of future types (stored directly) or an embedded environment without a heap allocator.

**Why `Output = ()`.** The executor's job is completion, not the value. `Mar::run` takes `F: Future<Output = ()>` and `Task::new` requires `Output = ()`, because the executor discards every task's result. Typed results do flow, but not through the task: `spawn_blocking` returns values through its own per-job channel to the awaiting future. This choice would change if the runtime offered a `spawn` API that returned a join handle yielding a typed value — that value would have to travel through the task's output or a channel, not by widening the erased output type.

**Why tasks stay in the table while `Pending`.** The table is the "alive" set; presence in the queue means "needs polling now". A `Pending` task must remain findable by its waker, which only knows an id. If a `Pending` task were dropped from the table, a later wake would find no entry to re-poll, and the `else { continue; }` path would silently skip it — the work would be lost without any error. Keeping the task in the table is what makes the id-based lookup work.

**Why `HashMap<TaskId, Task>` and not a list.** Tasks are removed when they complete, in arbitrary order, and looked up by id. A `HashMap` gives O(1) lookup and O(1) insert and remove. A `Vec` keyed by id would require dense ids or a scan to find a task, and as tasks complete out of order the slots left behind would either be wasted or force compaction. The map also makes the "alive" set explicit: membership in `tasks` is exactly the set of live tasks. This would change if task ids were dense slab indices, where a `Vec`-backed slab would be the idiomatic structure.

## 4. Failure Modes and Misconceptions

### What Breaks If Implemented Wrong

**Drop a `Pending` future instead of re-inserting it.** The task vanishes from the table while its waker may still be held by a timer or a socket. When the waker fires, the id is pushed, the lookup fails, and the `else { continue; }` skips it. `run()` returns while work is unfinished, silently violating the contract that `Mar::run` drives the future to completion. The drain's re-insertion on `Pending` is the code that prevents this.

**Move a `Task` after its first poll.** The future inside holds self-pointers into its own memory; moving the future would dangle them. This cannot happen in this codebase, because the future is pinned: `Pin` makes the move a compile-time error, not a runtime bug. That is the entire purpose of the `Pin` layer. The future pinned at `Box::pin` time stays at one heap address even as the `Task` struct moves in and out of the map.

**Let the task grow with the future.** If `Task` stored the future by value instead of behind a `Box`, a task holding a 64 KiB future would be a 64 KiB struct, and the task table and queue would copy it around on every move. The test `new_wraps_future_with_id` in `src/task/mod.rs` guards the invariant: it builds a task whose future holds `Box<[u8; 64 * 1024]>` and asserts `std::mem::size_of::<Task>()` equals exactly the five words the layout predicts — one word for the id, two for the waker's fat pointer, two for the trait object's fat pointer.

### Common Misunderstandings

**"`Pending` means the future is stuck."** A `Pending` future is parked, not broken. It is in the task table, waiting for its waker to push its id back onto the ready queue, at which point it will be polled again. Stuck is what happens when a future returns `Pending` without storing a waker — that is the bug, not the `Pending` itself.

**"Polling a future is just calling a function."** It is calling a function, but the function advances a state machine that must not be moved between calls. The state machine's local variables live in the struct, and a field may point at another field. `poll` takes `Pin<&mut Self>` precisely so the caller's obligation — don't move this — is enforced by the type system.

**"`Pin` and `Box` are the same thing."** They solve different problems. `Box` moves storage to the heap and gives a stable address. `Pin` forbids moving the pointee. A future is both heap-allocated (`Box`) and immovable (`Pin`); each layer is independently necessary, which is why the field is `Pin<Box<dyn Future>>` and not either alone.

**"The executor polls tasks on a timer."** The executor polls only what is in the ready queue. `Mar::run` parks the thread and fires timers, but the parking is the wait for a wake, not a polling schedule. A parked task is polled when its waker fires, and only then.

## 5. Summary

- A `Task` is the runtime's unit of work: an id, a waker, and a `Pin<Box<dyn Future<Output = ()>>>`.
- It exists because futures have unknown, unnameable types that must be stored, polled repeatedly, and identified when woken.
- It depends on `TaskId` for identity, `TaskWaker` for waking, and `RuntimeState::tasks` for storage.
- `spawn_root` creates tasks, `drain_ready_queue` polls and drops them, and external drivers wake them through their waker.
