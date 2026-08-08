# Reactor: How the Runtime Waits for Anything to Happen

## The Problem: Waiting Without Blocking

Picture an executor with three parked tasks. One is waiting for bytes to arrive on a network socket. Another is waiting for a timer to reach its deadline. A third is waiting for a blocking job running on a worker thread. None of them are ready right now. The ready queue is empty. But they will become ready — the socket will receive data, the deadline will pass, the worker will finish — and when they do, someone needs to notice and re-queue the right tasks. The question is: what does the executor do in the meantime?

The bad answers are tempting and familiar. It could spin — run a tight loop checking each source over and over, burning the CPU at a hundred percent, converting the cooperative scheduler into a space heater. It could sleep for a fixed interval — ten milliseconds, say — wake up, check everything, sleep again — which wastes CPU *and* adds avoidable latency to every response, because a task that became ready one microsecond after the executor fell asleep won't be polled for another nine. Neither approach scales, and neither respects the fact that the operating system already has machinery for exactly this job.

What's actually needed is to hand the OS a list of the things the executor cares about and say: "I'm going to sleep now. Wake me when *any* of these become ready, or when this much time passes — whichever happens first." The kernel suspends the thread entirely, monitors every registered source, and resumes it only when there's real work waiting. While suspended, the thread consumes zero CPU — not a fraction of a percent, not an idle spin, but truly nothing. The kernel is doing the watching, and the kernel is faster at it than any userspace loop could ever be.

This is I/O multiplexing, and the reactor is the part of the runtime that talks to it.

## The OS Primitives

Every major operating system provides a kernel structure for this exact purpose:

- **Linux:** `epoll` — create an epoll instance, register file descriptors with it, and call `epoll_wait` to block until something happens.
- **macOS/BSD:** `kqueue` — same idea, different API.
- **Windows:** IOCP — different shape, but the same fundamental capability.

The pattern is the same everywhere. The kernel is told "watch these file descriptors." The kernel stores the list in its own internal structures. One blocking call is made, and the kernel returns a batch of "these ones are ready."

This runtime doesn't talk to `epoll` or `kqueue` directly. It uses `mio`, a Rust crate that wraps these platform-specific APIs behind a uniform interface.

## The mio Types

`mio` provides four key pieces:

**`mio::Poll`** is the poller itself — the `epoll` instance on Linux. `poll.poll(&mut events, timeout)` blocks until something is ready or the timeout expires. It fills the `events` buffer with information about what happened.

**`mio::Registry`** is obtained from `poll.registry()`. It's the interface for registering and deregistering file descriptors. Code that only needs to register sources — without calling `poll` itself — can hold just a `Registry`.

**`mio::Token`** is a `usize` wrapper — a user-chosen label attached to each registration. When the poller returns an event, it includes the token. A network socket might be registered with token `Token(7)` and a pipe with token `Token(3)`. When the poller wakes the executor, the token tells *which* thing is ready. The kernel doesn't interpret the token — it's purely a userspace label.

**`mio::Events`** is a buffer that `poll.poll()` fills with ready events. It's allocated once with a fixed capacity and reused every iteration.

## The Wakeup Trick

`mio::Waker` is a special source. It's registered with the poller like any other file descriptor, but instead of representing a socket or pipe, it's a manual trigger. Calling `waker.wake()` from any thread causes the poller to return immediately, as if a file descriptor became ready.

This is how the worker pool tells the executor "a job finished." A worker thread, running on a completely different OS thread, calls `waker.wake()`. The kernel marks the waker's token as ready. If the executor is parked in `poll.poll()`, the kernel wakes it up with the waker's token in the events buffer. The executor then drains the completed-job channel.

Without this mechanism, a worker finishing a job on another thread would have no way to unblock the executor. The result would sit in a channel that nobody is reading until some other event coincidentally woke the executor.

## The Reactor Struct

```rust
pub struct Reactor {
    poll: mio::Poll,
}
```

One field. The reactor wraps `mio::Poll` and nothing else. There's no registration table in userspace — the kernel tracks what's registered. When the runtime registers a new source (currently just the `mio::Waker`), it calls `poll.registry()` to get the `Registry` and hands it to the registration method. The kernel does the rest.

The type alias is `ReactorHandle = Rc<RefCell<Reactor>>`. `Rc` gives shared ownership. `RefCell` gives interior mutability, which is needed because `poll(&mut self)` requires a mutable reference, but `Mar` holds the reactor behind a shared `Rc`. `RefCell::borrow_mut()` satisfies the `&mut self` requirement at runtime.

## What the Reactor Does

Two methods:

```rust
pub fn registry(&self) -> &mio::Registry {
    self.poll.registry()
}
```

Exposes the `Registry` so `Mar::new` can create the `mio::Waker`. The waker must be registered with the *same* `mio::Poll` instance that the executor will later call `poll` on. If the waker were registered with a different poller, calling `wake()` would signal nobody, and the executor would park forever.

```rust
pub fn poll(&mut self, events: &mut mio::Events, timeout: Option<Duration>) -> io::Result<()> {
    self.poll.poll(events, timeout)
}
```

Delegates to `mio::Poll::poll`. The `events` buffer gets filled. The `timeout` controls how long to block: `Some(Duration)` blocks up to that long; `None` blocks forever; `Some(Duration::ZERO)` returns immediately (non-blocking poll).

## Three Outcomes of `reactor.poll`

When the executor calls this, three things can happen:

**1. I/O events occurred.** One or more registered file descriptors became ready. The `events` buffer holds entries with tokens. The executor iterates over them. Currently only `WAKEN_TOKEN` (token 0, the pool's waker) is registered, so this always means "a worker finished something."

**2. Timeout expired, no events.** The duration elapsed and nothing became ready. The `events` buffer is empty. The executor proceeds to `fire_due_timers` — the timeout *was* the timer mechanism. The executor parked until the earliest deadline, and when it woke with an empty buffer, it knows it's time to fire expired timers.

**3. Both.** A worker finished *and* a timeout expired. The `events` buffer has `WAKEN_TOKEN` entries. The executor handles both: wakes completed-blocking tasks, fires due timers.

## What's Actually Registered Today

Right now, the reactor watches exactly one thing: the worker pool's `mio::Waker`. No sockets, no pipes, no user I/O.

This might seem like overkill — why wrap a whole OS multiplexer just for one wake signal? Because the reactor is *infrastructure*. The `registry()` method is the seam where a future I/O driver would register sockets. Building a full TCP/UDP driver is intentionally out of scope for this small runtime, but the plumbing is ready. When someone adds a socket, they call `registry.register(socket, Token(1), Interests::READABLE)` and the kernel starts watching it. The reactor doesn't change at all.

## Walking Through the Pool Wake

A worker finishes a job. Here's the full path:

1. **Worker wakes.** The worker thread calls `w.wake()` on its `Arc<mio::Waker>`. This `mio::Waker` was created in `Mar::new` with `mio::Waker::new(registry, WAKEN_TOKEN)`, registered with the same `mio::Poll` the reactor wraps.

2. **Kernel responds.** `wake()` makes a system call (`eventfd_write` on Linux) that tells the kernel "mark this source ready." Since the waker's file descriptor is registered with the `epoll` instance, `epoll_wait` returns immediately.

3. **Executor resumes.** `mio::Poll::poll` returns. The `events` buffer contains one event with `token() == WAKEN_TOKEN`.

4. **Executor processes.** `wake_completed_blocking` sees the token, drains the completed-id channel, looks up each `BlockingId` in `blocking_wakers`, and calls `wake_by_ref()` to push task ids onto the ready queue.

5. **Next drain.** The ready queue has the task's id. The executor polls it. The `BlockingTask` reads its result and returns `Ready`.

## Walking Through a Timer Wake

No worker is involved. Just a `Sleep` future:

1. **Executor drains.** The ready queue is empty. `is_done` finds a parked task and a timer entry. Not done.

2. **Compute timeout.** `compute_timeout` calls `wheel.next_deadline()`, gets `Some(deadline)` 1 second from now. Timeout is `Some(1s)`.

3. **Park.** `reactor.poll(&mut events, Some(1s))`. The thread blocks in the kernel. No file descriptors are ready, so the kernel waits.

4. **Timeout.** One second passes. `mio::Poll::poll` returns. The `events` buffer is empty — no I/O, just time.

5. **Fire timers.** `fire_due_timers` calls `wheel.expire_due()`. The expired entry's waker fires. Task id on the ready queue.

6. **Next drain.** Task polled. `Sleep` returns `Ready`. Done.

## Design Choices

**Why no userspace registration state?** The kernel stores the interest set internally. If a socket was registered with `epoll`, the kernel knows about it. When the socket is closed, the kernel removes it automatically. Mirroring this in a userspace `HashMap<Token, Source>` would duplicate kernel state and create a desync risk. The cost: when the executor receives an event with a token, it has no userspace map to tell it what that token means. In the current code, only `WAKEN_TOKEN` exists, so a simple `if` check suffices. A future I/O driver would bring its own token-to-waker map.

**Why `RefCell` instead of `Mutex`?** The reactor is accessed only from the executor thread. `RefCell` panics loudly on misuse; `Mutex` would add unnecessary locking overhead and hide bugs as silent deadlocks.

**What `WAKEN_TOKEN` means.** It's `mio::Token(0)`, a reserved token. The executor needs to distinguish "the pool woke me" from "a socket woke me." Token 0 is reserved so the pool's wake is unambiguous. `wake_completed_blocking` checks `if event.token() == WAKEN_TOKEN`.

## Common Misconceptions

**"The reactor polls futures."** The reactor doesn't know what a future is. It blocks the thread and produces OS events. The executor polls futures. The reactor's only job is to park the thread efficiently and wake it when something happens.

**"Parked means the thread is running a loop."** When `mio::Poll::poll` blocks with a timeout, the thread is inside a kernel system call (`epoll_wait`). It's not executing Rust. It's not consuming CPU. The kernel suspends the thread and resumes it only when an event arrives or the timeout expires.

**"An empty events buffer is an error."** It's a normal timer wakeup. The timeout expired, no I/O happened. The executor proceeds to fire timers. An empty buffer means "only time passed."

**"Most writes need the reactor."** OS write buffering makes most writes return immediately — the kernel copies data into a send buffer and returns control. The reactor is needed for *readiness waiting* — when the kernel's buffer is full and a write would block, or when waiting for data that hasn't arrived yet.

## Summary

The reactor wraps the OS poller (`epoll`/`kqueue`) behind `mio::Poll`. It blocks the thread once, inside the kernel, and returns events for any ready file descriptor or expired timeout. Currently it watches one thing: the worker pool's `mio::Waker`. The `registry()` method is the seam where sockets and other I/O sources would plug in. The executor couples its park timeout to the earliest timer deadline, so the OS's own timeout mechanism drives timer wakeups without a dedicated timer thread.

Source: `src/reactor.rs`
