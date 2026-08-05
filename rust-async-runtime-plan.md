Building a minimal, single-threaded async runtime using only `mio` and `std` is one of the best ways to understand how async Rust works under the hood. Without macros or high-level runtime crates (like `tokio` or `futures`), you interact directly with the standard library primitives (`std::future::Future`, `std::task::Waker`, `std::task::RawWaker`, `std::pin::Pin`) and `mio`'s OS polling primitives (`Poll`, `Events`, `Registry`, `Token`).

To get a simple, single-threaded runtime working, you need **6 core components** (the async I/O wrapper you build on top is the 7th).


> **Memory model decision (locked in, with one deliberate exception):** Pure `Rc` for **all runtime state on the executor thread**. The entire runtime — every future, the ready queue, the timer wheel, the reactor registry — is `!Send` and lives on one thread. No `Arc`, no `Mutex`, no channels *on the executor side*. This keeps the Waker vtable as simple as possible and the `Send`/`Sync` question disappears for everything the executor touches. **The single exception is the Phase 3 `spawn_blocking` boundary (section 7):** a worker thread must receive a closure and hand back a result, which requires `std::sync::mpsc` channels + a `mio::Waker` — the only `Send` types in the whole crate. Everything else stays `!Send`. (Trade-off: the *executor* can never grow into a multi-threaded scheduler — that's fine, that's the point. Workers offload blocking work; they never poll futures.)

---

## Project Scope & Milestone

- **Phase 1 (this plan):** Runtime core + timer wheel + a concurrency demo. **No networking.**
  - Done = an integration test that spawns several concurrent `sleep()` tasks plus a counter task that `yield_now()`s between increments, and asserts:
    - total wall-clock time ≈ the *max* sleep duration (proves the executor parks instead of busy-looping or serializing);
    - the counter hit its expected value (proves tasks interleave and re-poll correctly, deterministically via yields);
    - `run()` returns cleanly with no tasks left in the queue, task map, or timer wheel.
- **Phase 2 (follow-up plan, deferred):** Reactor I/O dispatch + `AsyncTcpStream` + an echo demo over `UnixStream::pair()`. The sections below on I/O registration and the reactor registry are written for Phase 2; Phase 1 uses `mio::Poll` only as the park-with-timeout primitive, with zero registered sources.
- **Phase 3 (follow-up plan, deferred):** `spawn_blocking` + a worker pool — the escape hatch that makes **file reads** (and any blocking call) non-blocking by running them on worker threads (section 7).
- **Deliberately excluded:** a multi-threaded *executor* (Phase 3 adds worker threads for blocking offload only — workers never poll futures), `JoinHandle`s returning values, macro wrappers like `#[runtime::main]`/`block_on` sugar beyond `run()`.

---

## Project Structure & Dependencies

```toml
[package]
name = "mini-async-runtime"   # or similar
edition = "2021"

[dependencies]
mio = { version = "0.8", features = ["os-poll", "net"] }  # default "log" kept on
log = "0.4"

[dev-dependencies]
env_logger = "0.11"
```

(`"os-poll"` makes `Poll`/`Registry` real instead of a panic-on-use shell; `"net"` enables `TcpStream`/`UnixStream` for Phase 2.)

A **lib crate** so integration tests in `tests/` compile against the public API the way a user would:

```
src/
  lib.rs        # ✓ done — re-exports: Runtime, RuntimeState, Task, waker
  runtime_state.rs # ✓ done — RuntimeState { queue, tasks, next_id } (blocking added in Phase 3)
  task.rs       # ✓ done — Task { id, future }
  waker.rs      # ✓ done — RawWakerVTable boilerplate, create_waker(), WakerData
  executor.rs   # ✓ done — Runtime { state, wheel } with spawn() + run() + 5 tests, derives Default, wheel fully wired
  timer_wheel.rs # ✓ done — TimerWheel, Sleep, YieldNow, free functions, thread-local helpers + 5 tests
  reactor.rs    # — Phase 2: registry map + dispatch (parking Poll + WAKEN token live here)
  tcp.rs        # — Phase 2: AsyncTcpStream wrapper over mio::net::TcpStream
  http.rs       # — Phase 2: HttpGetFuture (raw HTTP/1.1 GET over AsyncTcpStream)
  blocking.rs   # — Phase 3: WorkerPool, BlockingTask, spawn_blocking()
tests/
  core.rs       # — spawn + run integration tests (the milestone demo lives here)
  blocking.rs   # — Phase 3: worker/interleave integration tests (file-read milestone)
```

Each module carries `#[cfg(test)] mod tests` for its own unit tests; `tests/core.rs` is the black-box acceptance suite.

### Trace points (via `log` + `env_logger`)

`RUST_LOG=trace` should show the full wake cycle so a red test pinpoints the bug:

- `spawn(id={id})` — new task enqueued
- `wake(id={id})` — waker pushed an id onto the ready queue
- `poll_task(id={id}, result=Pending|Ready)` — after every future poll
- `park(timeout={Some(ms)|None}, due_timers={n})` — entering the mio poll
- `dispatch(token={token})` / `timer_expired(id={id})` — both wake paths
- `submit_blocking(id={id})` — a `BlockingTask` sent its job to a worker (Phase 3)
- `worker_done(id={id})` / `waken(token=WAKEN, pending={n})` — a worker finished; executor woken cross-thread (Phase 3)

Every trace line carries the task id; grepping one id across the log tells the whole story of a task's life.

---

## Error Handling Policy

- **No `unwrap()`/`expect()` in non-test code.** Every fallible operation returns `io::Result<T>` and propagates with `?`.
- `Runtime::new()` currently returns plain `Self` (no fallible operations yet). Step 7 changes this to `io::Result<Runtime>` when `mio::Poll::new()` is integrated.
- `run(&mut self)` → `io::Result<()>` (propagates `Poll::poll` errors).
- Phase-2 `AsyncTcpStream` methods (`register`/`read`/`write`/`deregister`) → `io::Result`.
- Infallible-by-construction operations (Rc allocations, `HashMap` inserts, wheel pops) stay plain — no invented `Result` types.
- The **only** remaining panics are invariant violations, and they must be *loud*:
  - A `RefCell` double-borrow (borrow conflict in the waker path) → panic. That's a bug in our wake discipline, and it's the exact bug we want a red test to catch.
  - A reactor event token with **no matching waker** in the registry → panic, not skip. A silent skip hides a mistriaged `Ready` set.
  - In contrast, a **stale id** popped off the ready queue that's no longer in the task map → *skip silently*. That's legitimate (a future woke itself then completed) and must not panic.
- Rationale: reading one `run()?` path end-to-end in `main` is clearer for a learning project than panic-on-everything, and the `?` marks every point where the OS can actually say no.

**Task panics (locked in):** if a spawned future panics mid-poll, the panic **propagates out of `run()`**. No `catch_unwind`. Rationale: catching it mid-drain leaves `RuntimeState` in an uncertain state, and for a learning runtime a panic is the loudest, most debuggable failure mode. (One test asserts exactly this: a panicking task makes `run()` panic.)

---

## 1. The Task Wrapper (`Task`)

A `Task` is the concrete container that holds a top-level `Future` and its metadata while it is being executed.

Because futures in Rust are self-referential state machines generated by the compiler, they cannot be polled unless they are pinned in memory.

**Contents:**

- `future`: `Pin<Box<dyn Future<Output = ()>>>` — The heap-allocated, pinned async block. (Important subtlety: `Pin<Box<...>>` pins the **heap allocation**, not the `Box` wrapper, so a `Task` can be moved by value — e.g. into/out of a `HashMap` — safely.)
- `id`: A unique `usize` identifier (which maps cleanly to `mio::Token(id)`).

Note: there is **no** `ready_queue` field on `Task`. The Waker carries the queue handle + this id, so `Task` stays minimal.

---

## 2. Manual Waker Implementation (`RawWaker` & `RawWakerVTable`)

Without helper crates like `futures::task::waker_ref`, you must manually build a `std::task::Waker` using Rust's low-level C-style virtual table (`RawWakerVTable`).

A `Waker` is essentially a type-erased pointer (`*const ()`) paired with 4 function pointers:

1. **`clone`**: Increments the reference count of your shared state and returns a new `RawWaker`.
2. **`wake`**: Takes ownership of the pointer, pushes the task's `id` into the Ready Queue, and decrements the reference count.
3. **`wake_by_ref`**: Same as `wake`, but operates on a reference without consuming the reference count.
4. **`drop`**: Decrements the reference count when the waker goes out of scope.

The pointer is an `Rc<WakerData>` leaked into `*const ()`, where `WakerData` bundles everything `wake` needs to reach the runtime:

```rust
// --- Models ---

// Phase 1 (current): derives Default; `blocking` field added in Phase 3.
struct RuntimeState {
    queue: VecDeque<usize>,
    tasks: HashMap<usize, Task>,
    next_id: usize,
}

// The type-erased payload behind a `RawWaker`.
struct WakerData {
    shared: Rc<RefCell<RuntimeState>>,
    id: usize,
}

// --- VTable entry points (signatures only) ---

const VTABLE: RawWakerVTable;

fn clone_raw(data: *const ()) -> RawWaker;
fn wake_raw(data: *const ());
fn wake_by_ref_raw(data: *const ());
fn drop_raw(data: *const ());

fn create_waker(shared: Rc<RefCell<RuntimeState>>, id: usize) -> Waker;
```

**Key insight:** `wake` only pushes the `id` into the queue — the future itself stays parked in the task table until the executor polls it. That's why the waker needs `WakerData`, not the `Task` itself.

---

## 3. RuntimeState (Ready Queue + Task Table)

Standardizing on pure `Rc` collapses the "ready queue" into one shared structure owned by the runtime:

```rust
// Phase 1 (current): derives Default; `blocking` added in Phase 3.
struct RuntimeState {
    queue: VecDeque<usize>,      // the ready queue: ids of tasks able to make progress
    tasks: HashMap<usize, Task>, // every live task, keyed by id
    next_id: usize,              // spawn counter (mio::Token(id) maps 1:1)
}
```

All three live behind a single `Rc<RefCell<RuntimeState>>` handed to spawners, wakers, and the executor. (Phase 3 adds a fourth field `blocking: HashMap<usize, Waker>`.)

- **`queue: VecDeque<usize>`** — Tasks are **not** pushed here by value; only their ids. When a task is newly spawned, or when an OS event / timer fires, its id gets enqueued.
- **`tasks: HashMap<usize, Task>`** — The future stays parked here between polls. The executor *removes* a task by id, polls it, and **re-inserts it if it returns `Pending`**. If it returns `Ready`, the task is dropped (done). Because `Pin<Box<dyn Future>>` pins only the heap allocation, this move-in/move-out of the map is safe.
- **Idempotent wakes:** a future may wake itself during poll (e.g. a timer that completes immediately on its second poll). The executor can then pop a stale id whose task is already gone — it must simply skip ids that don't resolve in `tasks`. Treat the queue as a *hint*, not a guarantee.
- **`blocking: HashMap<usize, Waker>` (Phase 3)** — The wakers of tasks currently parked on a worker result. The executor's `WAKEN` dispatch (section 7) wakes *every* entry here; a `BlockingTask` inserts on its first poll, removes itself on completion *and* on `Drop`. Nothing here ever crosses a thread boundary — workers only see the `mio::Waker`, never this map.

> **Why this shape?** The waker only knows an id, so a wake must not require the task to be reachable at a specific address. By keeping futures in a map and waking by id, `wake` can never dangle.

---

## 4. The Reactor (`mio::Poll` + Waker Registry)

The **Reactor** monitors file descriptors (sockets, pipes, timers) using the OS's native multiplexing (`epoll` on Linux, `kqueue` on macOS, `IOCP` on Windows) via `mio`.

> **Phase 1 vs Phase 2:** In Phase 1 the reactor is just `mio::Poll` used as a *parking device* — `poll(&mut events, timeout)` with zero registered sources. The `HashMap<Token, Waker>` registry, the `register` flow, and event dispatch below are all Phase 2 (deferred). Build the parking part now; the registry is additive later.

**Registry Mapping:** The Reactor maintains a `HashMap<mio::Token, Waker>` (or array) mapping OS event tokens to task `Waker`s.

**How non-blocking I/O interacts with it:**

1. An async I/O wrapper (e.g., custom `AsyncTcpStream` wrapping `mio::net::TcpStream`) attempts to `read()`.
2. If the OS returns `ErrorKind::WouldBlock`, the socket calls `mio::Registry::register()` with its file descriptor, a `mio::Token(task_id)`, and requested `Interest` (Readable/Writable).
3. It saves the task's `cx.waker()` into the Reactor's map and returns `Poll::Pending`.

**`mio::Waker`:** `mio::Waker` is a separate, thread-safe wake-up mechanism for the reactor (for cross-thread wakes / shutdown). Phases 1 & 2 never need it — every wake is same-thread. **Phase 3 builds it:** it is the one `Send` + `Clone` object a worker thread holds, and `wake()` makes a blocked `Poll::poll` return immediately with a reserved `WAKEN` token (section 7). It also remains handy for a clean `Runtime::shutdown()` signal.

---

## 5. The Timer Wheel (why the executor can park, and for how long)

`mio` has **no timer support** (it's explicitly omitted from the crate). So sleeping must be handled on the executor side, exactly the way tokio does it.

A `sleep()` future doesn't register anything with the OS. Instead, when first polled, it pushes `(deadline, task_id)` into a shared wheel and stores its waker:

```rust
// Same `Rc` family as `RuntimeState`
type TimerWheel = Rc<RefCell<BinaryHeap<Reverse<(Instant, usize)>>>>;
//                       priority keyed by deadline, so `peek()` = earliest
```

The **deadline is the contract between the timer future and the executor:**

1. Future polls, computes `deadline = Instant::now() + duration`, inserts `(deadline, id)` into the wheel, saves `cx.waker()`, returns `Pending`.
2. On a later poll (after being woken), the future checks `deadline <= now` → `Ready`; otherwise re-arms and stays `Pending`.
3. The executor never calls `sleep()` itself. When it parks (phase 2 below), it computes the timeout as `wheel.peek() → earliest_deadline - Instant::now()`, or `None` (block forever) if no timer is pending.
4. After `mio::Poll::poll` returns (for any reason), the executor drains the wheel: while the earliest deadline ≤ now, pop it and push its id onto the ready queue. The woken future's next poll sees the deadline has passed and completes.

**Timer cancellation (the `Drop` trap):** if a sleeping task is dropped before its deadline (executor shutdown, or a future abandons its `sleep`), its `(deadline, id)` would linger in the wheel forever and block the termination check (`wheel.is_empty()`). So `Sleep` must implement `Drop` that removes its own `(deadline, id)` entry from the wheel — mirroring `mio`'s `register`/`deregister` discipline. This is a small, high-value design detail and a good unit test: spawn a task that starts a long sleep then returns early; assert the wheel is empty after `run()`.

> **Why the wheel is worth building before sockets:** it exercises the executor's *park-and-wake* cycle end-to-end using only `std::time::Instant` — no OS fds involved. If your sleep test is green, the drain/park/dispatch skeleton is correct, and adding real I/O is purely additive.

---

## Public API (lib crate surface)

Everything `tests/` (and the demo `main`) uses is exactly this:

```rust
pub struct Runtime { /* state: Rc<RefCell<RuntimeState>>; derives Default. wheel, poll, ... added in later steps */ }

impl Runtime {
    pub fn new() -> Self;                                              // plain Self until step 7 (mio::Poll::new() adds io::Result)
    pub fn spawn<F>(&mut self, future: F) where F: Future<Output = ()> + 'static;
    pub fn run(&mut self) -> io::Result<()>;                           // blocks until done
}

// Free functions, tokio-style, backed by the thread-local installed by run():
pub fn spawn<F>(future: F) where F: Future<Output = ()> + 'static;
pub fn sleep(duration: Duration) -> Sleep;    // .await it inside any spawned task
pub fn yield_now() -> YieldNow;               // Pending once, wake-self, then Ready
pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>   // Phase 3
where F: FnOnce() -> R + Send + 'static, R: Send + 'static;
```

**Thread-local handle (locked in):** `run()` installs the runtime's `Rc<RefCell<TimerWheel>>` + task id allocator into a `thread_local!`, so `sleep()`, `spawn()`, *and* `spawn_blocking()` (Phase 3: + the pool's job sender) are all free functions with no parameters. This is literally what tokio's `SetCurrent` does — a nice "aha" for the reader. A task that wants to spawn more work calls the free `spawn()`; a task that wants to wait calls free `sleep()`; a task that wants to offload a blocking call calls free `spawn_blocking()`.

> `spawn` from *within* a task: works for free — it just enqueues an id into the shared state via the thread-local, and the drain loop picks it up next iteration. No channel needed. (One guard: the drain loop must not decide "no more work" based on the task map while it's mid-drain.)

## Test Strategy (TDD observability, locked in)

Two mechanisms, both `#[cfg(test)]`:

1. **Probe future** (`src/runtime_state.rs` or a `src/testutil.rs`): a test future that
   - records how many times it was polled,
   - captures `cx.waker().clone()` so a test can fire it manually,
   - can be told to return `Pending` N times then `Ready`,
   - can *wake itself* on a chosen poll (to exercise the stale-id path).
   This is the workhorse for waker and re-poll tests without any timing or I/O.

2. **State getters** on `RuntimeState` + the wheel, exposed `#[cfg(test)]` only:
   - `queue_len()` / `task_count()` / `wheel_len()` / `pending_ids()` — assert invariants directly,
   - `Rc::strong_count(...)` assertions after dropping a waker — catches the classic `Rc` leak where a leaked waker keeps a task alive forever (the #1 failure mode of manual vtables). If strong_count isn't back to the baseline, the test fails red: your `drop_raw` (or `wake_by_ref_raw`) is buggy.

> **Golden rule for this project:** *every* component ships with the test that proves its wake/refcount contract. The vtable step is complete only when the clone/wake/drop strong-count test passes; the executor step is complete only when the probe-future re-poll test passes; the timer step is complete only when the wheel-cancellation test passes.

### Timing-test policy (flake-proofing, locked in)

- Use **50–100ms** durations. Fast enough to keep the suite snappy, slow enough that the OS scheduler can't dent the assertions.
- **One-sided bounds only.** `elapsed >= duration` always holds, so assert that. The concurrency demo asserts `elapsed < max_duration * 2` (headroom for noise) — never `≈` and never a tight window.
- The counter-interleaving demo uses `yield_now()`, not sleeps, so its correctness assertion (exact counter value) has *no* timing component at all.
- If a timing test flakes once, it's a genuine bug in the park logic, not noise — investigate, don't loosen bounds.

---

## 6. The Executor Loop (The Driver)

The **Executor** is the main `loop` running on your single thread. It coordinates the **Ready Queue**, the **Timer Wheel**, and the **Reactor** in three distinct phases per iteration:

```
               ┌──────────────────────────────┐
               │     1. Drain Ready Queue     │
               │ (Run task.poll(&mut cx))     │
               └──────────────┬───────────────┘
                              │ Empty
                              ▼
               ┌──────────────────────────────┐
               │   2. Park / Sleep Phase      │
               │ (mio poll with timeout =     │
               │   nearest timer deadline)    │
               └──────────────┬───────────────┘
                              │ Events OR timeout
                              ▼
               ┌──────────────────────────────┐
               │  3. Dispatch + Expire        │
               │ (waker.wake() -> enqueue,    │
               │  expire due timers)          │
               └──────────────────────────────┘
```

### The 3-Phase Loop Execution:

1. **Drain Phase:** Pop task ids from the Ready Queue one by one. Resolve each id in `RuntimeState::tasks` (skip if missing — stale wake), construct a `Context` from the task's `Waker`, and call `task.future.poll(&mut context)`. `Pending` → re-insert into the task map; `Ready` → drop the task (done).
2. **Park / Sleep Phase:** When the Ready Queue is empty, compute the timeout from the **Timer Wheel** (`earliest deadline - now`, or `None` = block forever) and call `mio::Poll::poll(&mut events, timeout)`. The thread sleeps until I/O activity *or* the earliest timer fires.
3. **Dispatch + Expire Phase:** (a) Loop through the returned `mio::Events`; for each `mio::Token`, look up the matching `Waker` in the Reactor registry and call `waker.wake()` — this pushes ids into the Ready Queue. The reserved `WAKEN` token (Phase 3) means a worker finished: wake *every* waker in `RuntimeState::blocking` instead. (b) Drain the Timer Wheel of all due deadlines and enqueue those ids too.

### Termination condition

`run()` blocks until all tasks complete. Track outstanding work: `tasks.is_empty() && wheel.is_empty()` → plus `blocking.is_empty()` (Phase 3) and the reactor has no registrations → stop. (Simplest check: the executor stops when the queue is empty, the task map is empty, there are no registered I/O sources, and no task is parked on a worker.)

---

## 7. The Blocking Pool (`spawn_blocking`, Phase 3)

The one deliberate breach of the pure-`Rc` model — and the only way to make **file reads** (or any blocking call) non-blocking in this design. Regular files have no readiness signal on epoll/kqueue (they are always "ready"), so the OS can never tell the executor "the read finished." The fix: run the blocking call on a **worker thread**, and have the worker wake the executor when it is done. This is literally what tokio's `fs::read_to_string` is — a thread-pool wrapper around a blocking `std::fs` read.

**Public surface:**

```rust
pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
```

**The rule of the boundary:** everything on the executor thread stays pure `Rc`/`!Send` (memory-model note, top of this plan). The *only* `Send` things in the crate are what must cross threads: the job closure, its result, and the channels carrying them. **Workers never touch `RuntimeState`** — not by `Rc`, not by reference, not at all. They see exactly two things: the job channel and a `mio::Waker`.

**Components:**

- `WorkerPool { job_tx: mpsc::Sender<Job>, workers: Vec<JoinHandle<()>> }` — a fixed pool (default **1 worker thread**; a `Vec` you can size later). Each worker loops `job_rx.recv()`, runs the closure, and calls the shared `mio::Waker`. Workers exit when the channel closes.
- `mio::Waker` — registered once on the reactor `Poll` under a reserved `WAKEN` token (e.g. `Token(usize::MAX)`). It is the one object workers hold (`Send` + `Clone`). Its entire job: after a job finishes, `wake()` makes the executor's blocked `Poll::poll(timeout)` return *immediately* with the `WAKEN` token. Without it, the executor could park on `None` forever while a worker is mid-read.
- `BlockingTask<R>` — a future, the mirror of `Sleep` (same id/waker discipline, same `Drop` cleanup):
  1. **First poll:** allocate a `blocking_id`, create `(tx, rx) = std::sync::mpsc::channel::<R>()`, wrap the closure so it runs `f()` and sends the result through `tx`, push the job onto `job_tx`, save `cx.waker()` in `RuntimeState::blocking[blocking_id]`, return `Pending`.
  2. **Later polls:** `rx.try_recv()` — **never blocks, which is the whole point**. Result in → `Ready(result)` and remove the `RuntimeState::blocking` entry. Not yet → re-arm the waker, stay `Pending`.
  3. **`Drop`:** remove `RuntimeState::blocking[blocking_id]` — the same cancellation discipline as the timer `Drop` trap (section 5). A task that abandons its `BlockingTask` must not leave a stale waker behind.

**The wake path (two hops: worker → thread → tasks):**

1. Worker finishes → sends the result through `tx` → calls its `mio::Waker::wake()`.
2. The executor's `Poll::poll` returns with the `WAKEN` token (not a timeout, not an I/O event).
3. Executor dispatches `WAKEN`: for every `(id, waker)` in `RuntimeState::blocking`, call `waker.wake()` → ids land on the ready queue.
4. Re-polled `BlockingTask`s `try_recv()` their results and complete.

Waking *all* pending blocking tasks on `WAKEN` is slightly wasteful but trivially correct — blocking tasks are expected to be few. Optimize later if ever needed.

**While a worker is in flight, the executor may park indefinitely** (timeout `None` — no timers, no I/O pending) and rely on the `WAKEN` wake. That is exactly why the `WAKEN` token exists.

**Panic in a worker closure:** the worker runs the closure under `std::panic::catch_unwind` and sends the panic payload back through `tx` rather than letting it die on the worker thread. `BlockingTask` then `std::panic::resume_unwind`s it on the next poll — so the panic propagates out of `run()` exactly like any task panic (Error Handling policy). A panicking closure can never hang the task that awaits it.

**Termination & shutdown:** `run()` additionally requires `RuntimeState::blocking.is_empty()` before returning — a task parked on a worker is still outstanding work. On `Runtime` drop, close `job_tx`, let the workers drain the channel and exit, and `join` their handles. No worker threads leak.

**Trace points:** `submit_blocking(id={id})`, `worker_done(id={id})`, `waken(token=WAKEN, pending={n})`.

**Tests (TDD, red → green):**
- The milestone: spawn `spawn_blocking(|| std::fs::read_to_string(big_file))` **and** a `sleep(50ms)` task; assert the sleep fires while the worker is still reading. This is the direct, observable proof that other futures run while a worker blocks.
- Round-trip: the `BlockingTask` yields exactly the value the closure returned.
- Two concurrent `spawn_blocking` calls both complete (two workers in flight at once).
- Cancellation: a task that starts `spawn_blocking` then abandons it leaves `RuntimeState::blocking` empty after `run()`.
- Shutdown: after `run()`, every worker thread has exited (assert via `JoinHandle` completion / pool thread count).
- `spawn_blocking` called from *inside* a task (thread-local free fn, same as `spawn`).
- Worker panic: a panicking closure makes `run()` panic — never hang.

---

## Minimal Concrete Flow Example

Putting it together without macros (Phase 2 shape, note the `?` on `run()` per the error policy):

```rust
fn main() -> io::Result<()> {
    let mut runtime = Runtime::new()?;

    // Phase 2: custom HttpGetFuture over AsyncTcpStream (no reqwest, no tokio)
    runtime.spawn(async {
        match HttpGetFuture::get("httpbin.org/get").await {
            Ok(response) => println!("response: {response}"),
            Err(e) => eprintln!("http error: {e}"),
        }
    });

    // Phase 3: read a local file offloaded to a worker thread
    runtime.spawn(async {
        let contents = spawn_blocking(|| {
            std::fs::read_to_string("/etc/hosts").unwrap()
        })
        .await;
        println!("hosts file: {contents}");
    });

    runtime.run()?;
    Ok(())
}
```

---

## Build Order — Phase 1 (core + timers, TDD)

Each step is: **write the failing test → make it pass → refactor.** Tests live in `#[cfg(test)]` modules *next to* the code, and integration tests that exercise the whole `run()` loop live in `tests/`.

- [x] **Step 1** — Define the `Task` struct (`id` + `future`). *Test:* unit test for `next_id` monotonicity once spawn exists; nothing else to test yet — this step is mostly scaffolding.
- [x] **Step 2** — Build the shared state: `Rc<RefCell<RuntimeState>>` with queue, task map, and id counter. *Test:* push/pop the queue, insert/remove a task from the map, ids never repeat. `RuntimeState` derives `Default`.
- [x] **Step 3** — Implement the manual `RawWakerVTable` + `create_waker(shared, id)`. *Test (the one that teaches the most):* create a waker, clone it a few times, assert the shared `Rc` refcount goes up, `waker.wake()` enqueues the right id, and dropping everything brings refcount back to 1.
- [x] **Step 4** — Write the Executor core (spawn + `run()`), **poll-only** — drain loop + block-on-empty, no timers, no reactor yet. *Tests (5 total):* empty initial state; spawn id/registration; immediate return with no work; shared-counter run (3 tasks, 3 polls); `Probe` future self-waking re-poll tests the full wake→repoll cycle. `Runtime` derives `Default`. Parks with a fixed 100ms `PARK_TIMEOUT`; refactored to the wheel in step 6.
- [x] **Step 5** — Add the Timer Wheel + `sleep()` future + the `yield_now()` primitive. *Tests (5 total, in `timer_wheel.rs`):* wheel min-heap ordering; `sleep(0)` completes in runtime; two concurrent 100ms sleeps finish <200ms (parallel, not serial); `yield_now()` self-wakes then completes; dropped `Sleep` removes its entry from the wheel (the `Drop` test). All 17 tests pass, 0 clippy warnings.
- [ ] **Step 6** — Bring in `mio::Poll` as the park primitive (currently `thread::sleep(timeout)` — works correctly for Phase 1 with zero registered sources). The wheel is *already wired* into the executor: `runtime.wheel`, `install`/`set_current_id`/`clear_current_id`, park timeout from `next_deadline`, expire/termination gates. When mio lands, `thread::sleep` → `mio::Poll::poll`. *Test:* the **concurrency demo** — concurrent sleeps + a `yield_now()` counter task, asserting `elapsed >= max_duration`, `elapsed < max_duration * 2`, and the exact counter value (this is the milestone test).
- [ ] **Step 7** — Finalize `Runtime::new()` → `io::Result<Self>` (when `mio::Poll::new()` is added), `run()` already returns `io::Result<()>`, and audit that no non-test code `unwrap`s. Currently `new()` returns plain `Self` since no fallible ops exist yet.

## Build Order — Phase 2 (deferred follow-up)

- [ ] **Step 8** — Reactor dispatch: register a `mio::net::UnixStream::pair()` as a test source, drive events through the token→waker registry, assert a write on one end wakes the task polling the other. (Real fds, no network, no server.)
- [ ] **Step 9** — `AsyncTcpStream` wrapper: `WouldBlock` → `register` + store waker + `Pending`; readable/writable → `Ready`. *Tests:* round-trip a byte string through `UnixStream::pair()`, end-to-end through the runtime.
- [ ] **Step 10** — `HttpGetFuture`: raw HTTP/1.1 GET over `AsyncTcpStream` — connects, sends the request, registers read interest, accumulates the response, returns the body on EOF. *Tests:* fetch from a real HTTP server (e.g. local `python3 -m http.server` or `httpbin.org`).
- [ ] **Step 11** — Echo demo: concurrent reader task + writer task over the socket pair.

## Build Order — Phase 3 (`spawn_blocking` + workers, deferred)

- [ ] **Step 12** — Build `WorkerPool` in isolation (pure `std`, no runtime): a job channel + N worker threads, each `recv` → run → drop. *Tests:* a worker runs a closure and the result is observable; workers exit when the channel closes (no thread leaks).
- [ ] **Step 13** — Add `BlockingTask` + the `WAKEN` wake path. *Tests:* the milestone — `spawn_blocking(read big file)` interleaves with a `sleep(50ms)` task; round-trip value correctness; two concurrent `spawn_blocking`s both complete; a cancelled `BlockingTask` leaves `RuntimeState::blocking` empty (the Phase 3 `Drop` test).
- [ ] **Step 14** — Worker panic semantics: `catch_unwind` in the worker, ship the payload through the result channel, `resume_unwind` on the task's next poll. *Test:* a panicking closure makes `run()` panic — never hang.
- [ ] **Step 15** — Finalize shutdown: close the job channel and join the workers on `Runtime` drop. *Test:* after `run()`, every worker thread has exited.

---

## Invariants Checklist (what the tests are *really* guarding)

These are the properties the whole test suite exists to protect. If one breaks, the test for it is the shortest path to the bug:

1. **Waker lifetime balance.** Every `Rc<WakerData>` created by `create_waker` is dropped exactly once — `clone` increments, `wake`/`drop` decrement. No leaks, no double-drops. *Checked by:* strong-count tests after dropping wakers.
2. **Wakes never dangle.** A `Waker` outlives its task? No — wake pushes only the id; if the task is already gone, the id is skipped. *Checked by:* stale-id-skip test.
3. **A task is never polled while pinned elsewhere.** Futures live in the map or are removed (held by the executor) — never polled from two places. `Pin<Box>` moved by value is safe, but a borrow conflict here panics loudly. *Checked by:* the borrow discipline in the drain loop + tests that wake during poll.
4. **The executor never parks with work pending.** If the queue or wheel has live entries, it keeps draining — the park timeout is the *earliest* deadline, and wake paths enqueue ids it must see. *Checked by:* concurrency demo timing assertions.
5. **Termination is total.** After `run()`, queue, task map, wheel, and (Phase 3) the blocking map are all empty — nothing stranded. *Checked by:* state getters in every integration test's final assertion.
6. **No silent I/O mistriage.** An unmatched event token panics rather than being dropped (Phase 2). *Checked by:* the Phase 2 dispatch test.
7. **The blocking boundary is sealed (Phase 3).** Workers never touch `RuntimeState` — they see only the job channel and the `mio::Waker`. Results cross back through `mpsc`; nothing `!Send` ever leaves the executor thread. *Checked by:* the `BlockingTask` round-trip test + the fact that `WorkerPool` compiles without any reference to `RuntimeState`.
8. **A worker result is never lost or hung on.** A completed closure always produces a wake (`WAKEN`), and a panicking closure sends its panic back instead of vanishing. *Checked by:* the worker-panic test and the interleave test.

## Verification Workflow (day-to-day)

```bash
# Everything, in dependency order (red -> green):
cargo test                       # unit tests per module + tests/ integration suite

# Watch mode while building a component:
cargo watch -x test              # optional; or just: cargo test waker

# Debugging a failing wake/refcount test:
RUST_LOG=mini_async_runtime=trace cargo test -- --nocapture

# Milestone demo lives in tests/core.rs as the concurrency integration test:
cargo test --test core
```

Trace filter: `RUST_LOG=mini_async_runtime=trace` prints every `spawn`/`wake`/`poll_task`/`park`/`timer_expired`. If the strong-count test goes red, the trace shows the exact `clone`/`wake`/`drop` sequence that unbalances the count.

---

## Locked-in Decisions (summary)

| Decision                  | Choice                                                            | Why                                                       |
| ------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------- |
| Memory model              | Pure `Rc` on the executor thread; one `Send` boundary for Phase 3 | Minimal, `!Send`, vtable stays trivial                    |
| Timers                    | Executor-side `BinaryHeap` wheel                                  | mio has none; teaches park-timeout pattern                |
| Milestone                 | Sleep/concurrency demo; no networking                             | Networking deferred to Phase 2                            |
| File I/O / blocking calls | `spawn_blocking` worker pool (Phase 3)                            | Files have no readiness signal; workers offload the block |
| Error handling            | `io::Result` everywhere, no non-test `unwrap`                     | One readable error path                                   |
| Task panics               | Propagate out of `run()`, no `catch_unwind`                       | Loud, debuggable, simpler                                 |
| Crate layout              | Lib crate, per-module unit tests + `tests/`                       | Tests hit the public API                                  |
| Logging                   | `log` crate + `env_logger` dev-dep                                | Traceable wake cycles                                     |
| `sleep()` API             | Free fn via thread-local (tokio `SetCurrent` style)               | Clean demo code                                           |
| Observability             | Probe future + `#[cfg(test)]` state getters + strong counts       | Tests assert invariants directly                          |
| Extra primitive           | `yield_now()` included                                            | Deterministic interleaving demo                           |
| Timing tests              | 50–100ms, one-sided bounds only                                   | Flake-proof                                               |
| Demo/verify               | Tests only, no `examples/` binary                                 | `tests/core.rs` is the demo                               |
|                           |                                                                   |                                                           |
