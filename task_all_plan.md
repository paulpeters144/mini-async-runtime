# Plan: `mar::task::all` — Homogeneous Array Fan-out

## API

```rust
pub fn all<F: Future + Unpin, const N: usize>(futures: [F; N]) -> JoinAll<F, N>;
```

```rust
let [r1, r2, r3] = mar::task::all([
    spawn_blocking(|| compute_a()),
    spawn_blocking(|| compute_b()),
    spawn_blocking(|| compute_c()),
]).await;
```

- Input: `[F; N]` — all futures same type, same `Output`
- Output: `[F::Output; N]` — results in input order
- Constraint: `F: Unpin` (covers all existing futures: `BlockingTask`, `Sleep`, `rrest::GetFuture`)

## Files

| File | Action |
|---|---|
| `src/task/all.rs` | **New** — `JoinAll` struct + `Future` impl + `all()` constructor |
| `src/task/mod.rs` | Add `pub mod all;` + `pub use all::all;` |
| `examples/fan_out_results.rs` | Rewrite to use `task::all` |

## Internals

```rust
pub struct JoinAll<F: Future + Unpin, const N: usize> {
    futures:  [Option<F>; N],          // None = completed
    outputs:  [Option<F::Output>; N],   // stored results
    remaining: usize,
}
```

**Poll logic:**
1. `Pin::get_mut()` (safe — `JoinAll: Unpin` when `F: Unpin`)
2. Iterate all `Some(fut)` slots, `Pin::new(fut).poll(cx)` (safe — `F: Unpin`)
3. On `Ready(val)` → `outputs[i] = Some(val)`, `futures[i] = None`, `remaining--`
4. On `remaining == 0` → drain outputs into `[F::Output; N]` via `std::array::from_fn(|i| outputs[i].take().unwrap())`
5. Otherwise → `Poll::Pending`

Waker behavior: all sub-futures share the `JoinAll`'s task waker. Any wake re-enqueues the `JoinAll` for re-poll.

Drop: `Option` naturally drops remaining `Some(fut)`/`Some(output)`. `BlockingTask::drop` cleans itself from `blocking_wakers`.

## Tests

| Test | What |
|---|---|
| `all_single` | N=1 → `[42]` |
| `all_three_spawn_blocking` | `[u32; 3]` via `spawn_blocking`, verify order |
| `all_with_sleep` | Two `sleep(50ms)`, total ≈ 50ms (concurrent) |
| `all_zero` | N=0 → `[]` immediately `Ready` |

## No unsafe code

- `F: Unpin` → `Pin::new(&mut F)` is the safe constructor
- `JoinAll: Unpin` → `Pin::get_mut()` is safe
- `std::array::from_fn` builds output array safely
