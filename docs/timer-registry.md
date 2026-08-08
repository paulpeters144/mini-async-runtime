# TimerRegistry: How Async Sleep Works

## Sleeping Without Blocking

Code needs to wait for one second before continuing. `time::sleep(Duration::from_secs(1))` is called, and the mental model says the world pauses for one second while the rest of the program keeps going. That's the promise of async — tasks yield cooperatively without freezing the whole system. But the machine doesn't understand "wait a second" on its own. It needs a mechanism, and not every mechanism works.

The obvious but catastrophic approach is `std::thread::sleep`. It does exactly what it says: it puts the entire thread to sleep inside the kernel. For a single-threaded async runtime, that means the executor itself goes to sleep. Task polling stops. The timer registry stops. The reactor stops. Every parked task that was waiting for an I/O event or a worker thread freezes alongside the one that wanted the delay, because they all share the same thread and that thread is now unconscious for a full second. Cooperative scheduling has been traded for a hard freeze, and nothing in the runtime can stop it because there's no preemption — it's a cooperative system and the programmer just refused to cooperate.

The correct approach is to decouple the desire from the waiting. Instead of blocking the thread, the deadline is registered with the runtime, the waker is handed over, and `Pending` is returned. The executor keeps running other tasks while the second passes. When the deadline arrives, the runtime notices and wakes the task. The code experiences the passage of time, but the runtime never stops. This is what the timer registry makes possible — asynchronous waiting that doesn't block, backed by the same OS machinery that already handles I/O.

## The Idea

A registry of `(deadline, waker)` entries. Nothing fancy — no timer wheel, no priority queue, just a flat list. The executor checks it every iteration:

1. **Is there a timer?** → Find the earliest deadline.
2. **How long until then?** → Compute the remaining duration.
3. **Park the thread for that long.** → The OS itself handles the wait via the reactor's timeout mechanism.
4. **Deadline passed?** → Collect the expired entries, wake their tasks.
5. **Repeat.**

No dedicated timer thread. No busy-waiting. The OS's `epoll_wait` timeout *is* the timer mechanism. The runtime piggybacks on the same system call that handles I/O multiplexing.

## The Struct

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

**`entries`** — an unsorted list of pending timers. Each entry is a deadline, a registry-local id, and the waker of the task that should be notified when the deadline arrives. The list is unsorted because appends are O(1) and the only operations that need ordering are `next_deadline` (scan for minimum) and `expire_due` (scan for expired). For the typical number of concurrent timers this runtime handles — dozens, not thousands — the scan is fast enough.

**`next_id`** — a counter for registry-local ids. This id is separate from `TaskId`. One task can have multiple `Sleep` futures active at once (for example, a `select!` racing two different timeouts). The registry needs its own identity space to tell these entries apart.

**`TimerEntry::deadline`** — an `Instant`. `Instant` is Rust's monotonic clock — it only moves forward and isn't affected by NTP adjustments, daylight saving time, or someone changing the system clock. Subtracting two `Instant`s always gives the real elapsed time.

**`TimerEntry::id`** — assigned by `push`, used by `Sleep::Drop` for cancellation. When a `Sleep` future is dropped before its deadline fires (because the task was cancelled or a `select!` chose a different branch), it calls `remove(id)` to clean itself up.

**`TimerEntry::waker`** — the task's waker, cloned from the `Context` during `Sleep::poll`. When the deadline passes, the registry calls `wake_by_ref()` on this, pushing the task's id onto the ready queue.

## The Operations

### `push` — Register a Timer

```rust
pub(crate) fn push(&self, deadline: Instant, waker: Waker) -> usize {
    let id = self.next_id.get();
    self.next_id.set(id + 1);
    self.entries.borrow_mut().push(TimerEntry { deadline, id, waker });
    id
}
```

Read the next id, increment, append the entry, return the id. The caller (`Sleep::poll`) stores the returned id so it can cancel the entry later.

### `remove` — Cancel a Timer

```rust
pub(crate) fn remove(&self, target_id: usize) {
    self.entries.borrow_mut().retain(|entry| entry.id != target_id);
}
```

Remove all entries matching an id. O(n) scan, but it's only called on drop/cancellation, and timer counts are small.

### `is_empty` — Is There Any Pending Timer?

```rust
pub(crate) fn is_empty(&self) -> bool {
    self.entries.borrow().is_empty()
}
```

Feeds directly into `is_done` in the executor loop. The runtime can't return while timers are pending — that would mean dropping timers before their tasks had a chance to complete.

### `next_deadline` — How Long Until the Next Timer?

```rust
pub(crate) fn next_deadline(&self) -> Option<Instant> {
    self.entries.borrow().iter().map(|e| e.deadline).min()
}
```

Linear scan for the earliest deadline. Returns `None` if no timers exist. The executor turns this into a reactor timeout: if it's `None`, park forever. If it's some `Instant`, compute `deadline.saturating_duration_since(Instant::now())` — the result is the park duration, or `Duration::ZERO` if the deadline already passed.

### `expire_due` — Fire Expired Timers

```rust
pub(crate) fn expire_due(&self) {
    let now = Instant::now();
    let mut entries = self.entries.borrow_mut();
    let mut due = Vec::new();
    entries.retain(|e| {
        if e.deadline <= now {
            due.push(e.waker.clone());
            false  // remove from entries
        } else {
            true   // keep
        }
    });
    drop(entries);  // release the borrow BEFORE waking
    for w in due {
        w.wake_by_ref();
    }
}
```

Two phases. Phase one: borrow the entries, scan everything, collect the wakers of expired entries, and retain only the unexpired ones. Phase two: drop the borrow, *then* wake each waker.

The order matters. `wake_by_ref()` runs the waker's `Wake` impl, which pushes a task id onto the ready queue. If a waker's code re-entered the timer registry — for instance, a `Sleep`'s enclosing task called another `sleep` inside its poll — it would try to `borrow_mut()` the same `RefCell`. If the borrow from phase one were still held, that would panic. The collect-then-wake split makes the borrow window explicit and tiny, so re-entrancy is always safe.

## The Consumer: `Sleep`

`Sleep` is the future that `time::sleep(d)` returns:

```rust
pub struct Sleep {
    registry: Rc<TimerRegistry>,
    deadline: Instant,
    id: Option<usize>,
    done: bool,
}
```

**`registry`** — a handle to the shared timer registry, cloned from the thread-local context when `sleep()` is called.

**`deadline`** — `Instant::now() + duration`, computed at construction.

**`id`** — the registry-assigned id. `None` before the first poll — the entry isn't registered until the future is actually polled. This means `sleep(d)` never touches the registry unless it's inside `Mar::run` and actually `.await`ed.

**`done`** — guards against polling after completion. Once a `Sleep` returns `Ready`, subsequent polls return `Ready` immediately.

### `Sleep::poll`

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

Three branches, checked in order:

1. Already done? Return `Ready`.
2. Deadline passed already? (Happens with `sleep(Duration::ZERO)` or if the deadline came and went before this poll.) Return `Ready`.
3. Not registered yet? Register `(deadline, waker)` in the registry, store the id, return `Pending`.
4. Already registered? Just return `Pending` — the entry is already in the registry.

### `Sleep::Drop`

```rust
impl Drop for Sleep {
    fn drop(&mut self) {
        if !self.done && let Some(id) = self.id {
            self.registry.remove(id);
        }
    }
}
```

This is critical. If a `Sleep` is dropped before its deadline fires — because the task was cancelled, or a `select!` chose a timeout instead — the entry must be removed from the registry. A dead entry in the registry means the registry is never empty, `is_done` never passes, and `run()` hangs forever. This `Drop` impl is not an optimization; it's a termination requirement.

## Walking Through a Timer

Let's trace `sleep(Duration::from_secs(1))` from start to finish:

**t₀: Construction.** `sleep(1s)` reads the thread-local context, clones the timer registry, and returns `Sleep { deadline: now + 1s, id: None, done: false }`.

**t₁: First poll.** The executor polls the task containing this `Sleep`. `Sleep::poll`: deadline not reached, id is `None`, so it calls `registry.push(deadline, waker)`. The registry appends entry id 0. `Pending` returned.

**t₂: Drain ends.** The ready queue is empty. `is_done` sees the task table has the parked task and the registry has one entry. Not done.

**t₃: Park.** `compute_timeout` calls `next_deadline()`, gets `Some(t₀ + 1s)`. Timeout is roughly `Some(1s)`. The reactor blocks the thread for ~1 second.

**t₄: Timer fires.** `fire_due_timers` calls `expire_due()`. The entry's deadline is `<= now`. Its waker is collected, the entry removed, and `wake_by_ref()` pushes the task's id onto the queue.

**t₅: Second poll.** The drain polls the task. `Sleep::poll`: `Instant::now() >= deadline`, so `done = true`, `Ready(())` returned. Task completes.

**t₆: Done.** The task is gone, registry is empty. `is_done` passes.

## Precision Guarantees

Timers never fire *before* their deadline. `expire_due` checks `deadline <= now`, so an entry whose deadline is one nanosecond in the future survives the scan and waits for the next iteration.

Timers may fire *after* their deadline, bounded by one event-loop iteration. If the executor is busy polling tasks for 50ms after a deadline passes, the timer fires 50ms late. This bounded lateness is acceptable — the async contract is "wait at least this long," not "fire exactly at this nanosecond."

## Design Choices

**Why a `Vec` instead of a `BinaryHeap`?** A heap gives O(log n) push and O(log n) min-extract. But `expire_due` needs to find *all* expired entries, not just the minimum — that's still O(n) in a heap unless entries are popped repeatedly. For the small timer counts this runtime handles (dozens, not thousands), the flat `Vec` keeps the code simple and readable. The choice would change at scale.

**Why `RefCell` and not `Mutex`?** The timer registry is only accessed from the executor thread. `RefCell` panics loudly on misuse; `Mutex` would add locking overhead and hide bugs as silent deadlocks.

**What if a timer fires for a dropped task?** The waker pushes a stale id. The drain's lookup fails — `let Some(mut task) = ... else { continue; }` — and the id is skipped. Spurious wakes are harmless.

**What about `sleep(Duration::ZERO)`?** `Instant::now() >= deadline` is true on the first poll. `Ready` is returned immediately. The registry is never touched. No entry is created, no id is allocated.

## Common Misconceptions

**"Timers create sleeping threads."** Nothing sleeps but the executor's single park inside `mio::Poll::poll`. The timer registry is just a list in memory. The OS's timeout mechanism — the same `epoll_wait` call that handles I/O — does the actual waiting.

**"A timer fires exactly on time."** It fires at or after its deadline. A busy executor adds up to one iteration of lateness. It never fires early.

**"One timer per task."** One entry per `Sleep` future. A task with two concurrent sleeps (e.g., inside a `select!`) creates two entries with two different registry ids.

**"The waker in the registry runs the task."** The waker pushes an id onto the ready queue. The executor picks it up and polls the task. The registry never touches the task directly.

## Summary

The `TimerRegistry` is a flat list of `(deadline, id, waker)` entries. `Sleep` is the future that registers an entry on first poll and removes it on drop (to prevent the runtime from hanging). The executor couples `next_deadline` to the reactor's park timeout, so the OS's own wait mechanism handles timer expiration — no dedicated timer thread, no busy-waiting. `expire_due` uses a collect-then-wake pattern to safely fire wakers without holding a borrow.

Source: `src/time/timer_registry.rs`, `src/time/sleep.rs`
