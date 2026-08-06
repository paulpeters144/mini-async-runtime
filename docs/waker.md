# The waker (`mar::waker`)

Source: [`src/waker.rs`](../src/waker.rs)

A `Waker` is the mechanism by which a suspended future says *"something
happened — poll me again."* In this runtime, waking a task is literally pushing
its id onto the ready queue.

## What a `Waker` really is

`std::task::Waker` is a zero-sized type to the outside world: it carries one raw
pointer to *waker data* plus a vtable of four functions:

| Function | Job |
| --- | --- |
| `clone_raw` | clone the waker (increments a reference count) |
| `wake_raw` | wake, consuming the waker |
| `wake_by_raw_ref` | wake, borrowing the waker (may be called repeatedly) |
| `drop_raw` | release the waker's resources |

To *create* a waker you must supply these pieces yourself, via `RawWaker` +
`RawWakerVTable`. The std type is `Send + Sync`-agnostic; it just doesn't care
what the data pointer points at.

## Why build it by hand?

The standard `Waker` machinery is designed for multi-threaded runtimes, where
shared state is normally an `Arc` and must be `Send + Sync`. This runtime is
single-threaded by design, so all shared state lives in an `Rc<RefCell<_>>`.
Building the waker by hand lets the payload be an `Rc` instead of an `Arc` —
no atomics, no `Send + Sync` obligations — while still being a perfectly valid
`Waker`.

## The design

```rust
struct WakerData {
    state: Rc<RefCell<RuntimeState>>,  // the shared state (ready queue + maps)
    id: usize,                          // which task this waker represents
}
```

The vtable functions are thin:

- **wake:** `state.borrow_mut().queue.push_back(id)` — one line, but it's the
  heart of the runtime.
- **clone:** increments the `Rc` reference count (the raw-pointer dance in
  `clone_raw` does `Rc::from_raw` → `clone` → `Rc::into_raw` to bump the count
  while keeping the pointer alive).
- **drop:** `Rc::from_raw` and drop, decrementing the count.

`create_waker(state, id)` allocates a `WakerData`, leaks one `Rc` reference into
a raw pointer (`Rc::into_raw`), and wraps it in a `Waker`. Every `into_raw` must
eventually be matched by a `from_raw` — that accounting is what the
`refcount_stays_balanced` test watches step by step.

## Memory discipline

The trickiest part of a hand-built waker is memory safety:

- every `Rc::into_raw` leaks one reference into a raw pointer;
- every leaked reference *must* eventually be reclaimed with `Rc::from_raw` +
  drop;
- forget one → leak; reclaim the same pointer twice → use-after-free.

The `refcount_stays_balanced` test tracks `Rc::strong_count` as it goes
`1 → 2` (into the waker) `→ 3` (clone) `→ 2` (drop clone) `→ 1` (drop waker),
proving each clone was cleaned up exactly once.

## Waker discipline

The golden rule of this runtime: **a future that returns `Pending` must ensure
its waker will eventually be woken** — otherwise the task is lost and the loop
can never terminate. Each component that parks a task stores a waker somewhere:

- `Sleep` stores it in the future and re-polls itself when the wheel expires it;
- `Readable`/`Writable` store it in the reactor's token→waker registry;
- `BlockingTask` stores it in `RuntimeState::blocking`;
- `yield_now` and any self-waking future call `wake_by_ref()` immediately.

## Key tests to read

- `wake_push_id` — the full round trip: waker for id 7 → `wake()` → the id
  appears in the queue exactly once.
- `wake_by_ref_twice` — a single waker can wake repeatedly; each call pushes
  the id again.
- `cloned_waker_works` — clones share the same underlying data; waking through
  either reaches the same queue.
- `refcount_stays_balanced` — the memory-accounting test described above.
