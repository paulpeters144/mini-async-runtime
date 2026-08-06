# The reactor (`mar::reactor`)

Source: [`src/reactor.rs`](../src/reactor.rs)

The reactor is the runtime's connection to the OS. It wraps `mio::Poll` so the
executor can sleep until a socket (or any `mio::event::Source`) becomes ready,
instead of busy-polling. This is the single place in the crate where actual OS
I/O waits happen.

## What `Reactor` owns

| Field | Type | Role |
| --- | --- | --- |
| `poll` | `mio::Poll` | The OS poller; `poll.poll(events, timeout)` blocks the thread |
| `registry` | `HashMap<mio::Token, Waker>` | Maps an OS readiness token back to the waker of the task parked on that source |
| `next_token` | `usize` | The allocator for fresh tokens; starts at 1 |

### The token allocator

Every registered source needs a *unique* `mio::Token`, or two I/O futures would
collide on the same readiness event and wake the wrong task. `allocate_token()`
hands out fresh, never-reused tokens starting at 1. **`Token(0)` is reserved**
as `WAKEN_TOKEN` — the cross-thread wake signal used by the blocking worker
pool (see [blocking.md](blocking.md)).

### The registry

The registry is the reactor's memory: a map from `mio::Token` back to the
`Waker` of whichever task is parked on that source. `dispatch` consults it to
turn a raw readiness event into a `wake()` call.

## The two halves of the reactor

The reactor has a split personality, and the split matters:

1. **Registration side** — *who* is waiting. `register_source(source, token,
   interest)` registers an OS source with the poller; `register(token, waker)`
   records which task's waker fires when that token goes ready. `deregister` /
   `deregister_source` remove them.
2. **Event side** — *what happened*. `park(events, timeout)` blocks the thread
   on `mio::Poll::poll`. `dispatch(handle, events)` walks the returned events
   and, for each token, looks up the registered waker and calls `wake_by_ref()`.

These are called from different places: I/O futures register themselves during
their `poll`; the executor calls `park` when the ready queue is drained and
`dispatch` when `park` returns.

## Level-triggered readiness

`mio::Poll` is level-triggered: a source that stays ready keeps producing
events on every `park` until it is deregistered. Dispatching an event does *not*
remove the waker from the registry. Only an explicit `deregister` (the I/O
wrapper's `Drop`/completion discipline — see [io.md](io.md)) empties the
registry. That explicit cleanup is exactly what lets the executor's termination
check pass: `reactor.is_empty()` is true only when no task is parked.

## Panic on unmatched tokens

`dispatch` panics loudly if an event token has no registered waker. This is a
deliberate choice: a silent skip would hide a mis-attributed `Ready` set and
produce a mysteriously stuck runtime. If we ever see the panic, it's a bug in
our wake discipline.

## The `WAKEN_TOKEN` special case

`dispatch` *skips* `Token(0)`. The `WAKEN_TOKEN` event is handled separately by
the executor: when it appears, `run` wakes every waker in
`RuntimeState::blocking`. This is how a worker thread's completion reaches the
executor thread — through `mio::Waker`, which is specifically designed to fire
a token from any thread.

## Thread-local access

The reactor is accessible through the single thread-local `ContextHandle`
installed by `Mar::run`. I/O futures reach it through `context::with(|ctx| …)`,
which provides access to all runtime internals including the reactor. This is
why `io::read()` and `io::write()` are plain functions with no explicit
handle — and why calling them outside `run()` panics.

## Key tests to read

- `allocate_token_gives_distinct_tokens` — the allocator never hands out the
  same token twice.
- `registry_maps_token_to_waker_until_deregistered` — the registry's lifecycle
  and how `is_empty()` reflects it.
- `dispatch_wakes_the_task_registered_under_the_event_token` — the full cycle:
  a write on one end of a `UnixStream` pair → `park` returns → `dispatch` → the
  parked task's id lands on the ready queue.
- `dispatch_panics_on_an_unmatched_event_token` — a ready event with no waker
  is a loud bug, not a silent skip.
- `park_returns_early_when_a_source_is_ready` — `park` is event-driven, not a
  fixed sleep: a ready source returns immediately, never consuming the timeout.
- `events_keep_waking_until_the_task_deregisters` — level-triggered behavior,
  and why explicit deregistration matters.
