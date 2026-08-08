# Reactor: I/O Multiplexing with mio and the OS Poller

## 1. Concept: What Is I/O Multiplexing?

### The Core Concept

The reactor blocks the thread once, inside the kernel, and the operating system wakes it when any watched file descriptor becomes ready or a timeout elapses. Instead of blocking on each descriptor separately or busy-looping over all of them, the runtime makes a single system call and receives a batch of events that are ready right now. This is I/O multiplexing.

### The Problem

An async runtime watches many things simultaneously: network sockets, pipes, the worker pool's wake signal, and timer deadlines. Each of these is represented by a file descriptor (an integer handle the kernel uses to track an I/O resource). The runtime must wait for *any* of them to become ready, not just one.

A blocking read on a socket parks the thread until *that* socket has data. While the thread waits, every other task, every timer, and every other socket stalls. A busy-loop that polls each descriptor in sequence wastes CPU cycles on descriptors that are not ready. Neither approach works.

The runtime needs to block the thread *once* and be woken when *any* watched descriptor has data, or when a timeout expires — whichever comes first.

### The OS Primitives

Every operating system provides a kernel structure for multiplexed I/O. On Linux, it is `epoll`. On macOS and BSD, it is `kqueue`. On Windows, it is `wepoll`. The shape is the same across all of them: the kernel holds a set of (file descriptor → interest) registrations. The program tells the kernel "notify me when fd 7 becomes readable" or "notify me when fd 12 becomes writable." The kernel stores these registrations internally.

A single wait call — `epoll_wait` on Linux — blocks the thread until at least one registered file descriptor has an event, or until a timeout elapses. The kernel then returns the set of ready events. The program processes them and waits again.

`mar` uses `mio`, a Rust crate that wraps these platform-specific primitives behind a uniform API. The runtime never calls `epoll` or `kqueue` directly. `mio` handles the platform differences.

### Registration

Registration means telling the kernel "watch this file descriptor for this kind of readiness." Readiness is the condition that a read or write would not block. A socket with data in its kernel receive buffer is readable. A socket with space in its send buffer is writable.

Registration lives in the kernel, not in userspace. The kernel maintains the interest set as part of its internal state for the `epoll`/`kqueue` instance. The program does not need to keep a copy. When the program registers a new file descriptor, the kernel adds it to its set. When the program closes the descriptor, the kernel removes it. No userspace data structure is needed to track what is registered.

### `mio`

`mio` provides four key types:

- **`mio::Poll`** wraps the OS poller object (the `epoll` instance on Linux). It is the thing that blocks and returns events. Created by `mio::Poll::new()`.

- **`mio::Registry`** is obtained from `poll.registry()`. It is the interface for registering and deregistering file descriptors. Code that owns a `Registry` can add new sources to the poller without owning the `Poll` itself.

- **`mio::Token`** is a `usize` wrapper — a user-chosen identifier attached to each registration. When the poller returns an event, it includes the token so the caller can tell which source fired. The token is the program's own label; the kernel does not interpret it.

- **`mio::Events`** is a buffer that `poll.poll()` fills with the ready events. Each event carries its token. The buffer is overwritten on each call to `poll`.

### The Wakeup Mechanism

`mio::Waker` is a special source registered with the poller. Calling `wake()` on it from any thread causes the poller to return immediately, as if a file descriptor became ready. The waker's token appears in the `Events` buffer.

This is the mechanism the worker pool uses to unblock the executor. A worker thread finishes a blocking closure and needs to tell the executor "a result is ready." The worker calls `waker.wake()`. The kernel marks the waker's token as ready. The executor's parked `mio::Poll::poll` call returns with that token in the events. The executor then processes the completion.

### Why an Async Runtime Cannot Use Blocking Reads

A blocking read on a socket calls into the kernel and parks the thread until *that specific* socket has data. The thread cannot do anything else while it waits — it cannot poll other tasks, fire timers, or process completions from the worker pool.

An async runtime must park until *any* of its watched sources has data. The multiplexing wait — `epoll_wait`, `kqueue`, or `mio::Poll::poll` — is the only mechanism that blocks once and wakes on any of many sources. This is why the reactor exists: it is the single point where the thread blocks, and it is the single point where the thread wakes.

---

## 2. How This Runtime Implements the Reactor

### The Struct

From `src/reactor.rs`:

```rust
pub(crate) type ReactorHandle = Rc<RefCell<Reactor>>;

pub struct Reactor {
    poll: mio::Poll,
}
```

### Field by Field

**`poll: mio::Poll`** — the OS poller object. On Linux, this wraps an `epoll` file descriptor. On macOS, a `kqueue` file descriptor. `mio::Poll::new()` creates the kernel object and returns a handle to it.

There is no registration table in userspace. The kernel holds the interest set. The `Reactor` struct wraps the poller and nothing else. When the runtime needs to register a new source (a socket, the worker pool's waker), it calls `poll.registry()` to get the `Registry`, and registers through that. The kernel tracks the registration internally.

**`ReactorHandle`** is a type alias: `Rc<RefCell<Reactor>>`. `Rc` (reference-counted pointer) gives shared ownership — multiple parts of the code can hold a handle to the same `Reactor`. `RefCell` provides runtime-checked interior mutability, which is required because `poll(&mut self)` takes a mutable reference, but the `Mar` struct holds the reactor behind an `Rc` (a shared reference). `RefCell::borrow_mut()` satisfies the `&mut self` requirement at runtime. The borrow is scoped to the `poll` call; no user code runs while the borrow is held.

### Construction

```rust
pub fn new() -> Self {
    let poll = mio::Poll::new().unwrap();
    Reactor { poll }
}
```

`mio::Poll::new()` creates the OS poller. It returns `io::Result<mio::Poll>`. The `.unwrap()` is appropriate here: if the kernel cannot create an `epoll` or `kqueue` instance, the program cannot do I/O at all. There is no meaningful recovery. The program panics with the operating system's error message.

`Reactor::new()` is called by `Mar::new()` (`src/mar.rs`). At that point, no sources are registered. The poller exists but is watching nothing.

### Core Methods

```rust
pub fn registry(&self) -> &mio::Registry {
    self.poll.registry()
}
```

`registry()` exposes the `mio::Registry` so that `Mar::new` can create the `mio::Waker` for the worker pool. The waker must be registered with the *same* `mio::Poll` instance that the executor will later call `poll` on. If the waker were registered with a different poller, calling `wake()` would signal a poller that nobody is waiting on, and the executor would park forever.

```rust
pub fn poll(
    &mut self,
    events: &mut mio::Events,
    timeout: Option<Duration>,
) -> io::Result<()> {
    self.poll.poll(events, timeout)
}
```

`poll` delegates to `mio::Poll::poll`. The `events` parameter is a buffer that the kernel fills with ready events. The `timeout` parameter controls how long to block: `Some(Duration)` blocks up to that long; `None` blocks forever; `Some(Duration::ZERO)` returns immediately. The method returns `io::Result<()>` — an error only if the underlying system call fails, which is rare and unrecoverable.

### Three Outcomes of `reactor.poll`

When the executor calls `reactor.poll(&mut events, timeout)`, three things can happen:

**1. I/O events occurred.** One or more registered file descriptors became ready. The kernel fills the `events` buffer. Each `Event` in the buffer carries a `Token` identifying which source fired. The executor inspects these tokens. In the current codebase, only `WAKEN_TOKEN` (token 0) is registered, so the only event the executor handles is the worker pool's wake signal.

**2. Timeout expired with no events.** The `timeout` elapsed and no file descriptor became ready. The `events` buffer is empty. The executor proceeds to fire timers. This is how timer-driven wakeups reach the event loop: the executor parks until the earliest timer deadline, and when the timeout expires, `fire_due_timers` runs.

**3. `WAKEN_TOKEN` is present.** The worker pool's `mio::Waker` was signaled. A worker thread called `waker.wake()` after completing a job. The executor calls `wake_completed_blocking`, which drains the completed-id channel and wakes the specific tasks that finished.

### What This Reactor Actually Registers Today

In the current codebase, the reactor hosts exactly one registered source: the `mio::Waker` for the worker pool. No user I/O sources — sockets, pipes, or anything else — are registered. The `registry()` method exists as the seam where a future I/O driver would register sockets and other sources. That driver is intentionally out of scope for this runtime.

The reactor is the *infrastructure* for I/O multiplexing. It wraps the OS poller and provides the `poll` and `registry` methods. Building an I/O driver on top of it — one that registers sockets, maps tokens to wakers, and handles read/write readiness — is a separate layer of work.

### Worked Example: The Pool Wake

Trace the path from a worker finishing a job to the executor processing the completion.

1. A worker thread in the `WorkerPool` (`src/task/worker_pool.rs`) finishes running a closure. The worker calls `w.wake()`, where `w` is the `Arc<mio::Waker>` the worker received at construction. This `mio::Waker` was created by `mio::Waker::new(registry, WAKEN_TOKEN)` during `Mar::new`, registered with the same `mio::Poll` the reactor wraps.

2. `mio::Waker::wake()` makes a system call (`eventfd_write` on Linux) that tells the kernel "mark this source ready." The kernel marks the waker's file descriptor as readable. Since this file descriptor is registered with the `epoll` instance, the `epoll_wait` call that the executor is currently blocked inside returns immediately.

3. `mio::Poll::poll` returns `Ok(())`. The `events` buffer contains one event with `token() == WAKEN_TOKEN`.

4. The executor's `poll_readiness_events` (`src/mar.rs`) returns. The executor proceeds to `fire_due_timers` (no-op if no timers are due), then to `wake_completed_blocking`.

5. `wake_completed_blocking` iterates over `events`. It finds the event with `WAKEN_TOKEN`. It calls `runtime.pool.drain_completed()`, which returns a `Vec<BlockingId>` of all completed blocking jobs. For each `BlockingId`, it looks up the corresponding waker in `state.blocking_wakers` and calls `wake_by_ref()`. This pushes the task's id into the ready queue.

6. The next iteration of the executor loop calls `drain_ready_queue`, which pops the task id, looks up the task, and re-polls it. The `BlockingTask` reads its result channel, finds the result, and returns `Ready`.

### Worked Example: The Timer Path

Trace the path from an executor parking to a timer firing.

1. The executor drains the ready queue and finds it empty. `is_done` checks: the task table is non-empty (a `Sleep` future is parked), the timer registry is non-empty (it has one entry), `blocking_wakers` is empty. The executor is not done.

2. `compute_timeout` calls `wheel.next_deadline()`, which returns `Some(deadline)`. The timeout is `deadline.saturating_duration_since(Instant::now())` — the duration until the timer fires. Say the deadline is 1 second away, so the timeout is `Some(Duration::from_secs(1))`.

3. The executor calls `reactor.poll(&mut events, Some(Duration::from_secs(1)))`. The thread blocks inside the kernel. No file descriptors are ready (no worker has finished), so the kernel waits.

4. One second passes. The timeout expires. `mio::Poll::poll` returns `Ok(())`. The `events` buffer is empty — no file descriptor became ready; only the timeout fired.

5. The executor's `poll_readiness_events` returns. `fire_due_timers` calls `wheel.expire_due()`, which scans the timer registry, finds the entry with `deadline <= now`, collects its waker, and calls `wake_by_ref()`. The task's id is pushed into the ready queue.

6. The next iteration calls `drain_ready_queue`, pops the task, re-polls the `Sleep` future, which sees `Instant::now() >= deadline` and returns `Ready`.

### Interactions

- **`Mar::new`** (`src/mar.rs`) calls `Reactor::new()` to create the poller, then calls `reactor.borrow().registry()` to get the `Registry` for creating the `mio::Waker`.
- **`Mar::run`** calls `poll_readiness_events` every iteration, which calls `reactor.borrow_mut().poll(&mut events, timeout)`.
- **`WorkerPool` workers** (`src/task/worker_pool.rs`) call `w.wake()` on the `Arc<mio::Waker>` after every job. This signals the reactor from another thread.
- The interaction is bidirectional: `Mar` creates and polls the reactor; the worker pool wakes the reactor from other threads.

### Source Links

- `src/reactor.rs` — the `Reactor` struct and its methods.
- `src/mar.rs` — `Mar::new` (reactor creation, waker registration), `poll_readiness_events` (the poll call), `wake_completed_blocking` (token handling).
- `src/task/worker_pool.rs` — the worker loop that calls `w.wake()`.

---

## 3. Design Decisions

### Why No In-Userspace Registration State

The kernel stores the interest set. When the program registers a file descriptor with `mio::Registry::register`, the kernel adds it to its internal `epoll`/`kqueue` set. When the program closes the descriptor, the kernel removes it. The `Reactor` struct does not mirror this in a `HashMap<Token, Source>` or similar structure.

Storing a parallel map in userspace would duplicate kernel state and create a desync risk: if the program closes a file descriptor without deregistering it, the userspace map would still list it. The kernel handles this automatically — closing a descriptor removes it from the `epoll` set.

The cost of this choice: when the executor receives an event with a token, it has no userspace map to look up what that token corresponds to. In the current codebase this is not a problem because only `WAKEN_TOKEN` is registered, and the executor handles it with a simple `if` check. A future I/O driver that registers sockets would need its own token-to-waker map — but that map would be part of the driver, not the reactor.

### Why `RefCell` for `poll(&mut self)` Through a Shared Reference

`Mar` holds the reactor behind `Rc<RefCell<Reactor>>`. Calling `reactor.poll(&mut self)` requires a mutable reference. `RefCell::borrow_mut()` provides this at runtime. The borrow is scoped to the `poll` call — it starts when `borrow_mut()` is called and ends when the returned `RefMut` is dropped, which happens within `poll_readiness_events`.

A `Mutex` would provide the same `&mut self` access, but `Mutex` exists to protect data shared across threads. The reactor is only accessed from the executor thread. Using `Mutex` here would add unnecessary locking overhead and, worse, would hide borrow-order bugs as potential deadlocks instead of panicking loudly at the violating borrow.

### Why the Reactor Is Behind `Rc`

During `Mar::new`, the reactor is created first, then borrowed (`reactor.borrow()`) to get the `Registry` for creating the `mio::Waker`. The `Registry` reference must remain valid while `mio::Waker::new` runs. After construction, the worker pool holds an `Arc<mio::Waker>` — a reference-counted handle to the waker — but does not hold the reactor itself. The reactor is shared between `Mar` (which polls it) and the construction code (which registers the waker), but the pool only needs the waker, not the reactor.

### What `WAKEN_TOKEN` Is For

`WAKEN_TOKEN` is `mio::Token(0)`, a reserved token for the worker pool's `mio::Waker`. From `src/mar.rs`:

```rust
const WAKEN_TOKEN: mio::Token = mio::Token(0);
```

The executor needs to distinguish "the worker pool woke me" from "a file descriptor became ready." Token 0 is reserved so the pool's wake is unambiguously distinguishable from any future I/O event tokens. `wake_completed_blocking` checks `if event.token() == WAKEN_TOKEN` before draining the completed channel.

If the token were not reserved — if the pool's waker and a socket shared the same token space — the executor would not know whether to drain completions or read from a socket. Reserving token 0 makes the distinction trivial.

### When This Changes

Adding an I/O driver (for sockets, pipes, or other file descriptors) would add:

- Per-source registration: calling `registry.register(source, token)` for each socket.
- A token-to-waker map: when a socket event arrives, the executor looks up the token in the map to find the waker for the task waiting on that socket.
- Edge-triggered or level-triggered interest management: `mio` supports both, and the choice affects how the driver handles partial reads and writes.

The reactor itself would not change. It already wraps the OS poller and exposes `registry()` and `poll()`. The I/O driver would use these methods; it would not modify the reactor.

---

## 4. Failure Modes and Misconceptions

### What Breaks If the Executor Parked with `None` Timeout While Timers Existed

If `compute_timeout` returned `None` (meaning "no timeout, park forever") while the timer registry had entries, the executor would block inside `mio::Poll::poll` indefinitely. No file descriptor would become ready (no worker is running), and no timeout would expire (there is none). The timer's deadline would pass, but the executor would not wake to fire it. The `Sleep` future would never be re-polled, and `run()` would hang.

The defense is the coupling between `compute_timeout` and `next_deadline`: `compute_timeout` always derives the timeout from the earliest timer deadline. If a timer exists, `next_deadline` returns `Some`, and the timeout is `Some(duration)`. The executor never parks with `None` while timers exist.

### What Breaks If the `mio::Waker` Were Created on a Different Poll Instance

During `Mar::new`, the `mio::Waker` is created with `mio::Waker::new(registry, WAKEN_TOKEN)`, where `registry` comes from `reactor.borrow().registry()`. This binds the waker to the same `mio::Poll` instance that the reactor wraps.

If the waker were created on a different poll instance — say, a fresh `mio::Poll::new()` — calling `waker.wake()` would signal *that* poller, not the one the executor is parked on. The executor's `mio::Poll::poll` would never return. The program would hang.

The code enforces this by construction: `Mar::new` creates the reactor, borrows it to get the registry, and creates the waker in the same function. There is no way to accidentally use a different registry.

### What Breaks If the Reactor Is Dropped While the Worker Pool Still Holds the `mio::Waker`

The `mio::Waker` is an `Arc<mio::Waker>`, not a reference to the reactor. It holds its own file descriptor (on Linux, an `eventfd`). Dropping the reactor closes the `epoll` instance, but the waker's file descriptor remains open. Calling `wake()` on a waker whose poll instance is closed will succeed at the system-call level (the `eventfd_write` succeeds), but no `epoll_wait` will ever observe it. The wake is lost. If the executor were somehow still parked on that closed poll instance, it would get an error, not a wake.

In practice this does not happen because the reactor and the worker pool are both owned by `Mar`, and `Mar::drop` runs them in field-declaration order. The pool drops first (its `shutdown` closes the job channel and joins workers), then the reactor drops. By the time the reactor drops, no worker is alive to call `wake()`.

### Common Misunderstandings

1. **"The reactor polls futures."** The reactor does not know about futures. It blocks the thread and produces events. The executor polls futures. The reactor's only job is to park the thread efficiently and wake it when something happens.

2. **"Parked means the thread is running a loop in userspace."** When `mio::Poll::poll` blocks, the thread is inside a kernel system call (`epoll_wait` on Linux). It is not executing any Rust code. It is not consuming CPU cycles. The kernel suspends the thread and resumes it when an event arrives or a timeout expires.

3. **"An empty `Events` buffer after a timeout is an error."** It is a normal timer wakeup. The timeout expired, no file descriptor became ready, and the executor proceeds to fire timers. The empty buffer means "nothing I/O-related happened; only time passed."

4. **"Writes need the reactor."** OS write buffering makes most writes return immediately. The kernel copies the data into a send buffer and returns control to the program. The reactor is needed for *readiness waiting* — waiting until a file descriptor can accept a read or write without blocking. Writes that fill the buffer and block are rare and would use the reactor only if the program needed to wait for buffer space.

---

## 5. Summary

- The reactor wraps the OS poller (`epoll`/`kqueue`) behind `mio::Poll`. It blocks the thread once and returns events for any ready file descriptor or timeout expiration.

- The reactor currently registers only the worker pool's `mio::Waker` (token `WAKEN_TOKEN`). The `registry()` method is the seam where a future I/O driver would register sockets and other sources.

- The executor calls `reactor.poll()` every iteration. The timeout is derived from the earliest timer deadline. Worker threads call `waker.wake()` from other threads to unblock the executor when a blocking job completes.

- The reactor exists because an async runtime must block the thread *once* and wake on *any* readiness event. Blocking on individual descriptors or busy-looping would stall or waste CPU.
