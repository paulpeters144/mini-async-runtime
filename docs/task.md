# Tasks (`mar::task`)

Source: [`src/task.rs`](../src/task.rs)

A `Task` is the unit of work the executor owns: one future, tagged with an id so
a waker can reach it.

```rust
pub struct Task {
    id: usize,                        // where the waker sends wakeups
    future: Pin<Box<dyn Future<Output = ()>>>,  // the thing to poll
}
```

It makes exactly two promises to the executor:

1. **I can be polled** — `poll(&mut self, cx)` forwards to the future's poll.
2. **I remember where I left off** — because the future is *pinned*, resuming
   after a `Pending` is safe: any self-references inside the future's state
   machine are still valid.

## Why `Box::pin`?

Two problems solved at once:

- **`Box`** — the future lives on the heap, so a `Task` is always a small,
  fixed-size struct: one `usize` id plus a fat pointer (data pointer + vtable
  pointer) for the trait object. A future that captures a megabyte still fits
  in a `Task`.
- **`Pin`** — once polled, an `async fn` state machine may hold pointers into
  its own memory. If the `Task` ever moved, those self-pointers would dangle.
  Pinning makes the future immovable, so it is safe to come back and poll it
  again later.

The test `new_wraps_future_with_id` proves the smallness: it builds a future
holding a 64 KiB array and asserts the `Task` is exactly three words.

## Why `Output = ()`?

`Task` erases the future's output type to `()` because the executor doesn't do
anything with return values — a root future either finishes (`Ready(())`) or
suspends (`Pending`). Real results are plumbed through other channels
(`spawn_blocking` uses an `mpsc` result channel; the root future communicates
with the outside world through shared state like `Rc<Cell<_>>`).

## Lifecycle

- **Born:** `Mar::run` creates the root task with id `0`; every task is created
  with a fresh id from `RuntimeState::next_id`.
- **Lives:** the executor takes it out of the task map, polls it, and either
  puts it back (on `Pending`) or drops it (on `Ready`). Between polls the task
  sits in the task map, waiting for its id to appear on the ready queue again.
- **Dies:** dropping a `Ready` task is the only way tasks are removed. There is
  no explicit cancellation API — cancelling a task just means dropping its
  future, which is exactly what happens when the root future drops a child.

## Why the future is a trait object

`dyn Future<Output = ()>` lets a single `Task` type hold *any* future — your
root `async` block, a `Sleep`, a `Readable`, a `BlockingTask` — all behind one
pointer. The `+ 'static` bound means a task never borrows from the stack; it
owns everything it needs to run to completion on its own.

## Key tests to read

- `pending_then_ready_resumes_across_polls` — a future that returns `Pending`
  on the first poll and `Ready` on the second, proving the task survives
  suspension and resumes from where it left off.
- `poll_returns_ready_when_future_completes` — the simplest case: `async {}`
  finishes on the first poll.
