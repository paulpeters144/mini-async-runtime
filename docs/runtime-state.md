# Runtime state (`mar::runtime_state`)

Source: [`src/runtime_state.rs`](../src/runtime_state.rs)

`RuntimeState` is a tiny struct holding everything the executor must share while
it runs. It is deliberately boring — no logic, just data — so the interesting
bits of the runtime (the waker, the timer wheel, the reactor) all talk to one
another through one well-known object.

```rust
pub struct RuntimeState {
    pub queue: VecDeque<usize>,        // ready queue: task ids to poll next
    pub tasks: HashMap<usize, Task>,   // live tasks, keyed by id
    pub next_id: usize,                // the id counter for new tasks
    pub blocking: HashMap<usize, Waker>, // tasks waiting on worker results
}
```

`RuntimeState::new()` wraps it in `Rc<RefCell<_>>` and returns that handle; the
executor, the waker, and the blocking machinery all hold clones of the same
`Rc`.

## The ready queue (`queue`)

A `VecDeque<usize>` of task ids. It is the runtime's **to-do list**: any task
whose id is in here is due to be polled. Ids get pushed:

- at startup, for the root task;
- by a waker (`wake`/`wake_by_ref` push the id);
- by `expire_due`, when a timer fires;
- by `reactor::dispatch`, when an I/O source becomes ready;
- when a worker's cross-thread wake fires.

The executor pops from the front and polls. Because a poll can re-push its own
id (a self-wake), the queue is drained until it is empty again.

## The task map (`tasks`)

`HashMap<usize, Task>` holding every *suspended* task. When a task is polled it
is removed from the map; if it returns `Pending` it goes back in. A `Ready` task
stays removed — it's finished. `tasks` and `queue` together are how the
executor keeps straight which tasks exist (`tasks`) and which are ready to run
(`queue`).

## The id counter (`next_id`)

Task ids are never reused. The root task gets `0`; every subsequent task gets
`next_id`, then `next_id += 1`. Distinct ids are what keep wakers unambiguous:
a waker for task `7` can only ever mean task `7`.

## The blocking map (`blocking`)

`HashMap<usize, Waker>` of tasks that are waiting on a worker thread's result.
`spawn_blocking` inserts the waiting task's waker here; when a worker finishes,
the executor wakes *every* waker in this map. The `Drop` of a `BlockingTask`
removes its entry, so a cancelled blocking task can't hang the termination
check.

## Why `Rc<RefCell<_>>` instead of `Arc<Mutex<_>>`

The runtime is single-threaded by design. `Rc` gives cheap clones with no
atomic overhead, and `RefCell` gives interior mutability with a *panic* (not a
deadlock) if two pieces of code ever borrow simultaneously. All the "double
borrow" comments scattered through the code are exactly this trade-off working
as intended: it's a runtime bug, and the runtime would rather crash loudly than
corrupt state.

## Key tests to read

The tests in the other modules exercise `RuntimeState` indirectly. The one
direct assertion is `new_runtime_has_empty_queue_and_task_map` in
[executor.md](executor.md), which checks that a fresh runtime starts empty and
`next_id == 0`.
