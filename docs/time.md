# Timers (`mar::time`)

Source: [`src/time.rs`](../src/time.rs)

The timer module is the runtime's alarm clock: it answers *"what's the next
thing we're waiting on, and when?"* and fires wakeups when deadlines arrive. It
backs the `sleep()` free function.

## The data structure — `TimerHeap`

A **min heap** of `(deadline, id, Waker)` entries:

- `BinaryHeap` is a max-heap by default, so every entry is wrapped in
  `Reverse` to flip the ordering. The entry with the *earliest* deadline ends
  up at the top.
- Each entry stores a `Waker` inside the heap itself — `expire_due` calls
  `wake_by_ref()` directly on expired wakers instead of returning task ids.
  This eliminates the need for `current_id`; `sleep()` no longer cares which
  task is calling it.
- `TimerHeap` owns an internal `RefCell` and is shared as `Rc<TimerHeap>`
  between the executor and every `Sleep` future.

## `Sleep` — the future behind `sleep()`

`Sleep` is a future that completes after a duration:

- **First poll:** gets a fresh id from `TimerHeap`, pushes
  `(deadline, id, Waker)` into the heap, returns `Pending`.
- **Later poll:** (after the executor expired the deadline and the waker fired)
  sees `now >= deadline`, returns `Ready`.
- **Already past:** if the deadline has already passed by the first poll (e.g.
  `sleep(Duration::ZERO)`), it returns `Ready` immediately without touching the
  heap.

### The cancellation trap

The runtime's termination check requires the heap to be empty when all tasks
finish. If a `Sleep` is dropped *before* its deadline, its `Drop` impl scrubs
its entry out of the heap. Otherwise a stale entry would make the executor park
forever waiting for a timer that can never fire. This is the same discipline
used by the reactor and the blocking-waker map.

## `sleep()` — the free function

`sleep(duration)` is the ergonomic entry point. It reads the shared timer heap
from the thread-local context that `Mar::run` installed, then builds a `Sleep`.
Calling it outside `run()` panics with a clear message.

## How the executor uses the heap

The executor interacts through two functions:

| Function | What it does |
| --- | --- |
| `next_deadline` | Peeks the earliest deadline (or `None` if the heap is empty) so `run()` can compute its park timeout: `deadline − now` |
| `expire_due` | Pops every entry whose deadline has passed and calls `waker.wake_by_ref()` on each, which pushes the task's id onto the ready queue |

## `YieldNow` and `yield_now()`

Voluntary cooperation lives in `mar::task::yield_now` (`src/task/yield_now.rs`).
It builds a future that self-wakes: first poll sets a `done` flag and calls
`wake_by_ref()`, returning `Pending`; second poll returns `Ready`. No timer is
involved — it just re-queues the task for the *next* iteration of the drain
loop.

## Key tests to read

- `heap_is_min_heap_by_deadline` — proves the ordering: insert 100ms, 50ms,
  150ms; the heap pops the earliest first.
- `sleep_zero_completes_in_runtime` — `sleep(0)` completes immediately,
  first-past-the-deadline.
- `dropped_sleep_removes_itself_from_heap` — the cancellation trap: dropping a
  `Sleep` before its deadline leaves the heap empty.
- `yield_now_returns_pending_then_ready` (in `task/yield_now.rs`) — a task that
  `yield_now().await`s is polled exactly twice.
