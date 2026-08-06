# The timer wheel (`mar::timer_wheel`)

Source: [`src/timer_wheel.rs`](../src/timer_wheel.rs)

The timer wheel is the runtime's alarm clock: it answers *"what's the next
thing we're waiting on, and when?"* and fires wakeups when deadlines arrive. It
backs the `sleep()` and `yield_now()` free functions.

## The data structure

Despite the name, it's not a classic hierarchical timing wheel — it's a **min
heap**:

```rust
pub(crate) type TimerWheel =
    Rc<RefCell<BinaryHeap<Reverse<(Instant, usize)>>>>;
```

- `BinaryHeap` is a max-heap by default, so every entry is wrapped in
  `Reverse` to flip the ordering. The entry with the *earliest* deadline ends
  up at the top.
- Each entry is a `(deadline, task_id)` pair: which task to wake, and when.
- `Rc<RefCell<_>>` lets the executor and every `Sleep` future share the same
  heap on one thread — no `Arc`, no `Mutex`.

## `Sleep` — the future behind `sleep()`

`Sleep` is a future that completes after a duration:

- **First poll:** pushes `(deadline, id)` into the wheel, stores the waker,
  returns `Pending`.
- **Later poll:** (after the executor expired the deadline and woken the task)
  sees `now >= deadline`, returns `Ready`.
- **Already past:** if the deadline has already passed by the first poll (e.g.
  `sleep(Duration::ZERO)`), it returns `Ready` immediately without touching the
  wheel.

### The cancellation trap

The runtime's termination check requires the wheel to be empty when all tasks
finish. If a `Sleep` is dropped *before* its deadline, its `Drop` impl scrubs
its `(deadline, id)` entry out of the heap. Otherwise a stale entry would make
the executor park forever waiting for a timer that can never fire. This is the
same discipline used by the reactor and the blocking map.

## `sleep()` — the free function

`sleep(duration)` is the ergonomic entry point. It reads the *current task id*
and the *shared wheel* from the thread-local handle that `Mar::run` installed,
then builds a `Sleep`. Because `sleep()` runs during a poll, the executor has
already set the current id — see [executor.md](executor.md). Calling it outside
`run()` panics with a clear message.

## `yield_now()` — voluntary cooperation

`yield_now()` builds a future that self-wakes:

- **First poll:** sets a `done` flag, calls `wake_by_ref()`, returns `Pending`.
  The executor re-enqueues the task.
- **Second poll:** returns `Ready`.

No timer is involved — it just re-queues the task for the *next* iteration of
the drain loop, letting other ready tasks run first. This is the cooperative
building block for interleaving.

## How the executor uses the wheel

The executor interacts through three functions:

| Function | What it does |
| --- | --- |
| `next_deadline` | Peeks the earliest deadline (or `None` if the wheel is empty) so `run()` can compute its park timeout: `deadline − now`, or `0` if already due |
| `expire_due` | Pops every entry whose deadline has passed and returns their task ids, which `run()` pushes onto the ready queue |
| `set_current_id` / `clear_current_id` | Sets the thread-local "task being polled" id before/after each poll, so `sleep()` knows which task it belongs to |

The `set_current_id` mechanism is also what `spawn_blocking()` uses to learn
which task is waiting on a worker result (via `current_id()`).

## Key tests to read

- `wheel_is_min_heap_by_deadline` — proves the ordering: insert 100ms, 50ms,
  150ms; the heap pops 50ms first.
- `sleep_zero_completes_in_runtime` — `sleep(0)` completes immediately,
  first-past-the-deadline.
- `yield_now_returns_pending_then_ready` — a task that `yield_now().await`s is
  polled exactly twice.
- `dropped_sleep_removes_itself_from_wheel` — the cancellation trap: dropping a
  `Sleep` before its deadline leaves the wheel empty.
