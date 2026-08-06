# I/O futures (`mar::io`)

Source: [`src/io.rs`](../src/io.rs)

`io` provides the two futures that let user code await OS I/O without blocking
the executor thread: `io::read` and `io::write`. They are the primary consumers
of the reactor (see [reactor.md](reactor.md)).

## `Readable` — `io::read(src)`

`io::read(src)` takes ownership of a `mio::event::Source` (e.g. a
`mio::net::TcpStream`) and returns a future that reads bytes from it once. It is
the I/O analogue of `timer_wheel::sleep`: a free function that reaches the
shared reactor through the thread-local handle installed by `run()`.

The poll logic, step by step:

1. **Try the read.** Call `src.read(&mut buf)` (with `src` in non-blocking
   mode).
2. **`Ok(n)`** — data was available. Deregister the source, mark done, return
   `Poll::Ready(bytes)`.
3. **`Err(WouldBlock)`** — no data right now. This is the async heart:
   - on the first `WouldBlock`, register the source with the reactor for
     `Interest::READABLE` under a fresh token;
   - register this task's waker under that token;
   - return `Poll::Pending`. The executor will park the thread.
4. **Any other error** — panic. (In a real runtime this would be an error
   value; the demo keeps it loud.)

When the reactor's `dispatch` fires (a peer wrote data), the stored waker
re-queues the task. The next poll succeeds at step 2 and completes.

### Cancellation

If a `Readable` is dropped before completing, its `Drop` impl deregisters the
source from the poller *and* removes the waker from the reactor's registry.
This is the same cancellation discipline as `Sleep` in the timer wheel: a stale
registration would otherwise block the termination check forever.

## `Writable` — `io::write(src, buf)`

`io::write(src, buf)` writes a whole buffer to a source, returning when every
byte has been accepted. Writes are trickier than reads because the OS may
accept only *part* of a buffer before blocking:

1. **Try the write.** `src.write(&buf[offset..])`.
2. **`Ok(n)`** — advance `offset` by `n`. If the whole buffer is flushed,
   deregister and return `Ready`. Otherwise fall through to parking.
3. **`Err(WouldBlock)`** (or a partial write) — register the source for
   `Interest::WRITABLE`, store the waker, return `Pending`. The future remembers
   its `offset`, so the next poll resumes exactly where it left off.

The 1 MiB test in `io.rs` (`writable_partial_write_stores_waker`) is what
exercises this: a large payload overflows the socket's send buffer, forcing a
partial write and a parked `Writable` whose waker must be stored.

## Why one-shot?

Both futures **consume** the source they wrap and complete after a single
operation. This is a deliberate simplification: no per-call non-blocking setup,
no ownership gymnastics, no loop of reads. The `local_echo_server` example
works around the limitation by routing a request over one connection and the
reply over another — each socket sees exactly one operation.

## Interaction with the reactor

| Event | Reactor call |
| --- | --- |
| Future created | `allocate_token()` — reserve a unique token |
| First `WouldBlock` | `register_source(src, token, interest)` — put the fd on the OS poller |
| Every `Pending` poll | `register(token, waker)` — record who to wake |
| Read/write succeeds | `deregister_source(src, token)` — remove fd + waker |
| Dropped mid-flight | `deregister_source(src, token)` — same cleanup, from `Drop` |

All of these go through `reactor::with(|r| …)`, which borrows the shared
reactor for the duration of the call.

## Key tests to read

- `readable_returns_bytes_from_pair` — end-to-end: write to one end of a
  `UnixStream` pair, `io::read` the other, assert the bytes.
- `readable_from_an_unparked_source_completes` — data already buffered means no
  registration ever happens; the reactor stays empty.
- `writable_flushes_bytes_to_pair` — a small write completes on the first poll
  with no reactor involvement at all.
- `readable_and_writable_coexist_on_separate_pairs` — two active registrations
  at once, proving the token allocator prevents collisions.
- `writable_partial_write_stores_waker` — the partial-write case that forces a
  parked `Writable`.
