# The executor (`mar::Mar`)

Source: [`src/mar.rs`](../src/mar.rs)

`Mar` is the runtime itself: the public entry point, the owner of all shared
state, and the single-threaded event loop that makes futures progress. Everything
else in the crate exists to serve this loop.

## What `Mar` owns

| Field | Type | Role |
| --- | --- | --- |
| `state` | `Rc<RefCell<RuntimeState>>` | The ready queue, task map, and blocking map (see [runtime-state](runtime-state.md)) |
| `wheel` | `TimerWheel` | The shared deadline heap (see [timer-wheel](timer-wheel.md)) |
| `reactor` | `ReactorHandle` | The `mio::Poll` wrapper and token→waker registry (see [reactor](reactor.md)) |
| `pool` | `WorkerPool` | The blocking worker threads (see [blocking](blocking.md)) |
| `events` | `mio::Events` | Reused buffer that `mio::Poll::poll` fills with readiness events |

Note that `Mar` is `!Send`: every field is either `Rc<RefCell<_>>` or owned by
the single thread that runs `run()`. This is a deliberate design choice — see
the [README](../README.md).

## `Mar::run(future)` — the whole lifecycle

`run` is a static method; you never build a `Mar` yourself. The signature is
`run<F: Future<Output = ()> + 'static>(future: F) -> io::Result<()>`.

1. **Allocate.** `Mar::new()` builds the empty runtime: a fresh `RuntimeState`,
   an empty wheel, a fresh reactor, and a `WorkerPool`. The pool is given a
   cross-thread `mio::Waker` (registered under `WAKEN_TOKEN`) so workers can
   wake the executor thread.

2. **Install thread-locals.** `run` stashes the wheel, the reactor, and the
   blocking handle into thread-local storage. This is what lets free functions
   like `sleep()`, `io::read()`, and `spawn_blocking()` reach runtime internals
   without being passed explicit handles. See [thread-local plumbing](#thread-local-plumbing).

3. **Seed the root task.** Your future becomes task id `0`, stored in the task
   map and pushed onto the ready queue.

4. **Drain, park, wake — forever, until done.** This is the core loop,
   described below.

5. **Return.** When the termination check passes, `run` returns `Ok(())`.
   Dropping the `Mar` closes the worker channel and joins the worker threads.

## The event loop

The loop has two phases that alternate.

### Phase 1 — drain the ready queue

Pop task ids off the ready queue one at a time. For each:

- remove the task from the task map;
- tell the timer wheel which task is about to be polled (`set_current_id`);
- build a waker for this task;
- poll the task with that waker:
  - `Pending` → put the task back in the map (it will be woken later);
  - `Ready` → drop it (the future finished);
- clear the current task id.

A task can re-enqueue itself *during* a poll (that's what `wake_by_ref()` does),
so the drain loop keeps pulling until the queue is empty again.

### Termination check

After the queue is drained, the executor asks whether *everything* is empty:

- the ready queue,
- the task map,
- the timer wheel,
- the reactor's token→waker registry,
- the blocking-waker map.

If all five are empty, no task can ever make progress again, so `run` returns.
This is why every component has a **cancellation discipline**: dropping a
`sleep`, an I/O future, or a blocking task removes its entry from the shared
state, otherwise the loop would park forever waiting for a wake that never comes.

### Phase 2 — park

If the loop isn't done, the executor must wait for something to happen. It asks
the timer wheel for the earliest pending deadline and converts it into a timeout
(`deadline − now`; `0` if already past, or no timeout at all if the wheel is
empty). Then it hands `mio::Events` and that timeout to the reactor's `park`,
which blocks the thread on `mio::Poll::poll`.

`poll` returns when any of these happen:

- a deadline expired → `expire_due()` pops the due entries and pushes their task
  ids back onto the ready queue;
- an I/O source became ready → `reactor::dispatch()` looks up each event token
  in the registry and calls that task's waker;
- a worker finished a job → the `mio::Waker` fires with `WAKEN_TOKEN`, and `run`
  wakes every task in the blocking map.

Either way, task ids land back on the ready queue, and the loop goes to Phase 1.

## Thread-local plumbing

Three thread-locals are installed at the start of `run` and used by the free
functions:

| Thread-local | Installed by | Consumed by |
| --- | --- | --- |
| timer wheel handle | `timer_wheel::install` | `sleep()`, `yield_now()`, `spawn_blocking()` (for the current task id) |
| reactor handle | `reactor::install` | `io::read()`, `io::write()` |
| blocking handle | `blocking::install` | `spawn_blocking()` |

This is the same pattern tokio uses (`SetCurrent`): free functions stay
ergonomic, and the wiring stays invisible to user code. Calling a free function
outside of `run()` panics with a clear message rather than silently doing the
wrong thing.

## Panic safety

The blocking thread-local is wrapped in a `Drop` guard: if a task poll panics
(e.g. a blocking closure's payload is resumed inside `poll`), unwinding still
releases the thread-local job sender. Without this, dropping the `WorkerPool`
would block forever trying to join workers whose channel never closed.

## Key tests to read

- `new_runtime_has_empty_queue_and_task_map` — allocation starts clean.
- `probe_future_re_polls_until_ready` — the golden-rule test: a `Pending` task
  that self-wakes is re-polled exactly as many times as needed.
- `run_wakes_a_task_parked_on_io_readiness` — the whole stack (executor +
  reactor + waker) in one `run` call.
- `runtime_drop_joins_workers_promptly_after_run` — `run()` returning promptly
  *is* the assertion that workers shut down cleanly.
