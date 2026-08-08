# TimerRegistry: Deadline Tracking and Async Timers

## 1. Concept: What Are Async Timers?

### The Core Concept

An async timer registers a deadline with the runtime and returns `Pending`. The thread keeps running other tasks until the deadline passes, when the runtime wakes the task. `std::thread::sleep` blocks the whole thread — the executor can run nothing else while a task sleeps, which defeats async entirely. In async code, "sleep" must mean: register a deadline, return `Pending`, and get woken later.

### The Problem

Consider a task that needs to wait one second:

```rust
std::thread::sleep(Duration::from_secs(1));
```

This parks the entire executor thread for one second. No other task runs. No timers fire. No I/O is processed. The executor is frozen. The thread is doing nothing useful — it is blocked in the kernel, waiting for a clock interrupt. This is the opposite of what an async runtime needs.

The runtime needs a mechanism where a task says "wake me in one second," the runtime records that request, and the thread continues running other work. When the second passes, the runtime wakes exactly that task.

### The Solution Shape

A registry of `(deadline, waker)` entries. The executor needs to know the *earliest* deadline so it can park the thread until then. When time passes, the expired entries' wakers fire and their tasks re-poll. The executor does not need a dedicated timer thread or a timer wheel — it recomputes the park timeout each iteration from the current earliest deadline.

### How `sleep()` Composes

`time::sleep(d)` returns a `Sleep` future. On the first poll: if the deadline has already passed, return `Ready`; otherwise register `(deadline, waker)` in the registry and return `Pending`. A later poll — after the executor expired the deadline and woke the task — sees `Instant::now() >= deadline` and returns `Ready`. The future is a two-state machine: "waiting" and "done".

An `Instant` is a monotonic clock timestamp from the standard library. It is unaffected by wall-clock changes, NTP adjustments, or daylight saving time. Subtracting two `Instant` values always gives the real elapsed time, which is why `deadline.saturating_duration_since(Instant::now())` is safe.

### The Poll-Timeout Coupling

The executor's park timeout is not fixed. Each iteration, `compute_timeout` calls `wheel.next_deadline()` to get the earliest `Instant`, then computes the remaining duration. If no timers exist, the timeout is `None` (park forever until an I/O event). If a deadline passed during the drain, the timeout is `Duration::ZERO` (don't park at all — fire the timer immediately). Otherwise it is `Some(remaining)`. This coupling means the runtime waits for timers without a dedicated timer thread — the OS's own timeout mechanism does the waiting.

### Precision

Timers never fire *before* their deadline — `expire_due` checks `deadline <= now`, so an entry whose deadline is one nanosecond in the future survives the scan. Timers may fire up to one event-loop-iteration late: if the executor is busy polling tasks, it does not check timers until the drain finishes. This bounded lateness is correct — the contract is "at least this long," not "exactly this long."

## 2. How This Runtime Implements TimerRegistry

### The Struct

`src/time/timer_registry.rs`:

```rust
pub(crate) struct TimerRegistry {
    entries: RefCell<Vec<TimerEntry>>,
    next_id: Cell<usize>,
}

struct TimerEntry {
    deadline: Instant,
    id: usize,
    waker: Waker,
}
```

### Field by Field

**`entries: RefCell<Vec<TimerEntry>>`.** An unsorted list of pending timers. `RefCell` provides interior mutability on a single thread — `push` and `expire_due` call `borrow_mut()`, `is_empty` and `next_deadline` call `borrow()`. The `Vec` is unsorted because appends are O(1) and the registry only needs ordering when it scans (for `next_deadline` and `expire_due`). Keeping it unsorted avoids the cost of maintaining sorted order on every insert.

**`next_id: Cell<usize>`.** A counter for registry-local ids. `Cell` provides interior mutability for `Copy` types without runtime borrow checking — `get()` reads and `set()` writes, with no possibility of a double-borrow panic. The counter starts at 0 and increments on every `push`.

**`TimerEntry::deadline: Instant`.** The time at which this timer expires. `Instant` is a monotonic timestamp — it only moves forward and is not affected by system clock changes.

**`TimerEntry::id: usize`.** Assigned by `push`, used by `Sleep::Drop` for cancellation. This id is registry-local, not a `TaskId`. One task can have multiple simultaneous `Sleep` futures — for example, `select!` racing two timers — so the registry needs its own identity space to distinguish entries.

**`TimerEntry::waker: Waker`.** The task's waker, stored so `expire_due` can wake the task directly when the deadline passes. The waker was cloned from the `Context` during `Sleep::poll`.

### Core Methods

**`push`:**

```rust
pub(crate) fn push(&self, deadline: Instant, waker: Waker) -> usize {
    let id = self.next_id.get();
    self.next_id.set(id + 1);
    self.entries.borrow_mut().push(TimerEntry { deadline, id, waker });
    id
}
```

Reads the current id, increments it, appends the entry, returns the id. The caller (`Sleep::poll`) stores the id so it can call `remove` later.

**`remove`:**

```rust
pub(crate) fn remove(&self, target_id: usize) {
    self.entries.borrow_mut().retain(|entry| entry.id != target_id);
}
```

Removes all entries whose id matches `target_id`. `retain` is O(n) — it scans the entire vector. This is called by `Sleep::Drop` and is acceptable because timer counts are small.

**`is_empty`:**

```rust
pub(crate) fn is_empty(&self) -> bool {
    self.entries.borrow().is_empty()
}
```

Feeds the executor's termination check. `is_done` in `Mar::run` requires the registry to be empty before the runtime can return.

**`next_deadline`:**

```rust
pub(crate) fn next_deadline(&self) -> Option<Instant> {
    self.entries.borrow().iter().map(|e| e.deadline).min()
}
```

Linear scan for the minimum deadline. Returns `None` if the registry is empty. The executor turns this into a timeout: `deadline.saturating_duration_since(Instant::now())` produces `Duration::ZERO` (overdue), `Some(duration)` (remaining time), or the `None` propagates as "park forever."

**`expire_due`:**

```rust
pub(crate) fn expire_due(&self) {
    let now = Instant::now();
    let mut entries = self.entries.borrow_mut();
    let mut due = Vec::new();
    entries.retain(|e| {
        if e.deadline <= now {
            due.push(e.waker.clone());
            false
        } else {
            true
        }
    });
    drop(entries);
    for w in due {
        w.wake_by_ref();
    }
}
```

Two-phase expiry. Phase one: borrow the entries, scan the full list, collect the wakers of every entry whose deadline has passed, and `retain` only the unexpired entries. Phase two: `drop(entries)` releases the `RefCell` borrow, *then* calls `wake_by_ref()` on each collected waker.

The borrow must be released before waking. `wake_by_ref()` runs the `Wake` impl, which pushes the task's id onto the ready queue. If the executor is running `expire_due` from inside `Mar::run`, the `RefCell` on `RuntimeState` is not currently borrowed, so the push succeeds. But if a waker's code re-entered the timer registry — for example, a future that calls `sleep` inside its `poll` when woken — it would call `borrow_mut()` on the same `RefCell`. If the entries borrow were still held, this would panic. The collect-then-wake split makes the borrow window explicit and tiny.

### The Consumer: `Sleep`

`src/time/sleep.rs`:

```rust
pub struct Sleep {
    registry: Rc<TimerRegistry>,
    deadline: Instant,
    id: Option<usize>,
    done: bool,
}
```

**`registry: Rc<TimerRegistry>`.** A reference-counted handle to the shared timer registry, cloned from the `ContextHandle` by `sleep()`.

**`deadline: Instant`.** When this timer expires. Set to `Instant::now() + duration` at construction.

**`id: Option<usize>`.** The registry-assigned id, set on first `poll`. `None` before the first poll — the entry is not registered until the future is actually polled.

**`done: bool`.** Guard against polling after completion. Once `Ready` is returned, subsequent polls return `Ready` immediately.

**`Sleep::poll`:**

```rust
fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let this = self.get_mut();
    if this.done { return Poll::Ready(()); }
    if Instant::now() >= this.deadline {
        this.done = true;
        return Poll::Ready(());
    }
    if this.id.is_none() {
        let id = this.registry.push(this.deadline, cx.waker().clone());
        this.id = Some(id);
    }
    Poll::Pending
}
```

The `done` guard handles polling after completion. The deadline check handles `sleep(Duration::ZERO)` — the first poll sees the deadline already passed and returns `Ready` without touching the registry. On the first poll (when `id` is `None`), the future registers itself: calls `registry.push(deadline, cx.waker().clone())`, stores the returned id, and returns `Pending`. On subsequent polls, the id is already set, so it just returns `Pending` — the entry is already registered.

**`Sleep::Drop`:**

```rust
impl Drop for Sleep {
    fn drop(&mut self) {
        if !self.done && let Some(id) = self.id {
            self.registry.remove(id);
        }
    }
}
```

The cancellation trap. If a `Sleep` is dropped before its deadline fires — because the task was cancelled, or a `select!` chose a different branch — the entry must be removed from the registry. If the entry were left behind, the registry would never be empty, `is_done` would never pass, and `run()` would never return. `Drop` exists to prevent this; it is a termination requirement, not an optimization. The test `dropped_sleep_removes_itself_from_heap` in `src/time/sleep.rs` guards this invariant: it polls a `Sleep` once (inserting the entry), drops it, and asserts the registry is empty.

### `sleep(d)`

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

Calls `context::with` to read the `ContextHandle` from the thread-local, clones the `wheel` field (the `Rc<TimerRegistry>`), and builds the `Sleep`. `sleep(Duration::ZERO)` is valid: the first poll sees `Instant::now() >= deadline` and returns `Ready` without ever touching the registry.

### Worked Example

Trace `sleep(Duration::from_secs(1))` with concrete values.

**t₀: Construction.** `Instant::now()` returns `t₀`. `sleep(1s)` returns `Sleep { deadline: t₀ + 1s, id: None, done: false }`.

**t₁: First poll.** `Sleep::poll` runs. `done` is `false`. `Instant::now()` is `t₀ + ~0ms`, which is `< t₀ + 1s`. `id` is `None`, so `registry.push(t₀ + 1s, waker_clone)` is called. The registry appends `TimerEntry { deadline: t₀ + 1s, id: 0, waker }`. The id `0` is stored. `Pending` is returned. State: registry has one entry (id `0`, deadline `t₀ + 1s`).

**t₂: `is_done` check.** The registry is non-empty. `is_done` returns `false`.

**t₃: Park.** `compute_timeout` calls `wheel.next_deadline()`, which returns `Some(t₀ + 1s)`. The timeout is `(t₀ + 1s).saturating_duration_since(Instant::now())`, which is roughly `Some(1s)`. `reactor.poll` blocks the thread in the kernel for ~1 second.

**t₄: Timer fires.** `fire_due_timers` calls `wheel.expire_due()`. `Instant::now()` is now `t₀ + ~1s`. The entry's deadline `t₀ + 1s <= now`, so its waker is collected, the entry is removed, and `wake_by_ref()` pushes the task's id onto the ready queue. State: registry empty.

**t₅: Second poll.** `drain_ready_queue` pops the task and polls. `Sleep::poll`: `Instant::now() >= deadline`, so `done = true` and `Ready(())` is returned. The task completes.

### Source Links

- `src/time/timer_registry.rs` — `TimerRegistry`, `TimerEntry`, `push`, `remove`, `is_empty`, `next_deadline`, `expire_due`
- `src/time/sleep.rs` — `Sleep`, `Sleep::poll`, `Sleep::Drop`, `sleep()`
- `src/mar.rs` — `compute_timeout`, `fire_due_timers`

## 3. Design Decisions

**Why `Vec` and not a `BinaryHeap`.** A binary heap gives O(log n) push and O(log n) min-extract, but the code needs full scans for `expire_due` — which must find *all* expired entries, not just the minimum. A heap's advantage applies only to `next_deadline`, and even there the improvement is marginal. N is small in practice (dozens of timers); the `Vec` keeps the code simple. This would change at thousands of concurrent timers, where the O(n) scans dominate.

**Why wakers are called after the reactor poll, not during it.** `expire_due` runs after `poll_readiness_events` returns. The two-phase collect-then-wake avoids holding the `RefCell` borrow across `wake_by_ref` calls — a waker's code might re-enter the registry. Wakers run only after the thread has returned from the kernel, so the re-queued tasks are drained at the top of the next iteration.

**Why `RefCell` and not `Mutex`.** The timer registry is accessed only from the executor thread. A `Mutex` would add locking overhead and hide borrow-order bugs as deadlocks instead of panicking loudly.

**What happens if a timer fires for a dropped task.** The waker pushes an id; the drain's `else { continue; }` skips the missing task. This is a spurious wake — harmless because wake is a no-op for a nonexistent task.

**When this changes.** Precision-critical timers would move to a separate timer thread or a timing wheel. A heap or a hierarchical timing wheel would serve high timer counts.

## 4. Failure Modes and Misconceptions

### What Breaks If `Sleep::Drop` Did Not Remove Its Entry

The registry would never empty. `is_done` would require `wheel.is_empty()`, which would never hold. `run()` would hang forever, waiting for a timer that will never fire because the task that owned it is gone. The `dropped_sleep_removes_itself_from_heap` test guards this: it polls a `Sleep`, drops it, and asserts the registry is empty.

### What Breaks If `expire_due` Woke Wakers While Holding the Borrow

A waker whose wake path re-entered the timer registry — a future that calls `sleep` inside its `poll` when woken — would call `borrow_mut()` on the same `RefCell` while the entries borrow is held. The `RefCell` would panic with a borrow error. The collect-then-wake split prevents this.

### Common Misunderstandings

**"Timers create sleeping threads."** Nothing sleeps but the executor's single park inside `mio::Poll::poll`. The timer registry is a list of entries in memory; the OS's timeout mechanism does the actual waiting.

**"A timer fires exactly on time."** It fires at or after its deadline, bounded by one event-loop iteration. If the executor is busy polling tasks for 50ms after a deadline passes, the timer fires 50ms late. It never fires early.

**"One timer entry per task."** One entry per `Sleep` future, identified by the registry's own id. A task with two concurrent `Sleep` futures has two entries.

**"`sleep(0)` registers a timer."** `sleep(Duration::ZERO)` completes on the first poll. `Instant::now() >= deadline` holds immediately, so `Ready` is returned without ever touching the registry.

## 5. Summary

- The `TimerRegistry` is an unsorted `Vec` of `(deadline, id, waker)` entries that tracks when async timers expire.
- `Sleep` is the future that registers entries on first poll and returns `Ready` when the deadline passes. Its `Drop` removes the entry to prevent the registry from never emptying.
- `expire_due` collects expired wakers and wakes them after releasing the borrow, to avoid double-borrow panics.
- The executor couples the registry to its park timeout via `next_deadline`, so the thread blocks in the kernel until the earliest timer is due.
