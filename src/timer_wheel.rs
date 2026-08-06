use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// A shared priority queue of `(deadline, task_id)`, ordered so the entry
/// with the **earliest** deadline is always at the top.
///
/// `BinaryHeap` is a max-heap by default — it keeps the *largest* element at
/// the top. Wrapping every entry in `Reverse` flips the ordering: the
/// *smallest* `(Instant, …)` (i.e. the deadline that fires first) rises to
/// the top. `Rc<RefCell<…>>` lets the executor and every `Sleep` future share
/// the same heap on a single thread without `Arc` or `Mutex`.
pub(crate) type TimerWheel = Rc<RefCell<BinaryHeap<Reverse<(Instant, usize)>>>>;

// Thread-local storage that bridges free functions (`sleep()`, `yield_now()`)
// to the runtime without passing explicit handles.
//
// `run()` calls `install(…)` to stash the wheel here.  Before polling a task
// the executor calls `set_current_id(id)` so that `sleep()` can discover
// *which task* called it.  After the poll, `clear_current_id()` is called.
// This is the same pattern that tokio uses with its `SetCurrent` thread-local
// — free functions stay ergonomic, the wiring stays invisible.
thread_local! {
    static HANDLES: RefCell<Option<WheelHandle>> = const { RefCell::new(None) };
}

/// The data parked in `HANDLES` by `install()`.
struct WheelHandle {
    /// Every `Sleep` future pushes its (deadline, id) here.
    wheel: TimerWheel,
    /// Set by the executor just before a task is polled; read by `sleep()` to
    /// learn which id the created `Sleep` belongs to.
    current_id: Cell<Option<usize>>,
}

/// A future that completes after a given duration has elapsed.
///
/// On the **first** poll it pushes `(deadline, id)` into the shared wheel and
/// returns `Pending`.  On a later poll (after the executor expires the
/// deadline and wakes the task) it sees `Instant::now() >= deadline` and
/// returns `Ready`.  If the deadline has already passed by the first poll
/// (e.g. `sleep(0ms)`), it returns `Ready` immediately without touching the
/// wheel.
///
/// **Cancellation trap:** if a `Sleep` is dropped before the deadline fires,
/// its `Drop` impl removes its entry from the wheel so the executor's
/// termination check (`wheel.is_empty()`) is not blocked by a stale entry.
pub struct Sleep {
    wheel: TimerWheel,
    deadline: Instant,
    id: usize,
    waker: Option<Waker>,
    done: bool,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        if this.done {
            return Poll::Ready(());
        }
        if Instant::now() >= this.deadline {
            this.done = true;
            return Poll::Ready(());
        }

        if this.waker.is_none() {
            this.wheel
                .borrow_mut()
                .push(Reverse((this.deadline, this.id)));
            this.waker = Some(cx.waker().clone());
        }

        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if !self.done {
            let mut entries: Vec<Reverse<(Instant, usize)>> =
                self.wheel.borrow_mut().drain().collect();
            entries.retain(|Reverse((_, id))| *id != self.id);
            self.wheel.borrow_mut().extend(entries);
        }
    }
}

/// Pause the current task for at least `duration`.
///
/// This is a free function — it uses the thread-local `HANDLES` installed by
/// `Runtime::run()` to discover the current task's id and the shared wheel.
/// Calling it outside `run()` panics with a clear message.
///
/// `sleep(Duration::ZERO)` is valid and completes immediately on the first
/// poll without ever entering the wheel.
pub fn sleep(duration: Duration) -> Sleep {
    let (wheel, id) = HANDLES.with(|h| {
        let handles = h.borrow();
        let handles = handles.as_ref().expect("sleep() called outside of runtime");
        let wheel = handles.wheel.clone();
        let id = handles.current_id.get().unwrap();
        (wheel, id)
    });
    Sleep {
        wheel,
        deadline: Instant::now() + duration,
        id,
        waker: None,
        done: false,
    }
}

/// A future that voluntarily yields the executor's attention.
///
/// On its first poll it self-wakes via `wake_by_ref()` and returns `Pending`.
/// The executor re-enqueues the task and polls it again; the second poll
/// returns `Ready`.  This is the cooperative building-block for interleaving:
/// a task that calls `yield_now().await` lets other ready tasks run before
/// continuing.
pub struct YieldNow {
    done: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            Poll::Ready(())
        } else {
            self.done = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Cooperatively yield to the executor so other tasks can make progress.
///
/// Call `.await` on the result to suspend the current task exactly once.  No
/// timer is involved — the future self-wakes, so the task is re-polled on the
/// next iteration of the executor's drain loop.
pub fn yield_now() -> YieldNow {
    YieldNow { done: false }
}

/// Install a shared wheel into the thread-local `HANDLES` so free functions
/// like `sleep()` can reach it.
///
/// Called once by `Runtime::run()` at the start of execution.
pub(crate) fn install(wheel: TimerWheel) {
    HANDLES.with(|h| {
        *h.borrow_mut() = Some(WheelHandle {
            wheel,
            current_id: Cell::new(None),
        });
    });
}

/// Tell the thread-local handle which task id is currently being polled.
///
/// The executor calls this immediately before `task.poll(…)` so that
/// `sleep()` can retrieve the caller's id.
pub(crate) fn set_current_id(id: usize) {
    HANDLES.with(|h| {
        h.borrow()
            .as_ref()
            .expect("runtime handles not installed")
            .current_id
            .set(Some(id));
    });
}

/// Clear the current task id after a poll completes.
///
/// The executor calls this immediately after `task.poll(…)` returns.
pub(crate) fn clear_current_id() {
    HANDLES.with(|h| {
        h.borrow()
            .as_ref()
            .expect("runtime handles not installed")
            .current_id
            .set(None);
    });
}

/// Return the current task id — set by the executor before each poll.
///
/// Used by `spawn_blocking()` to learn which calling task is waiting on
/// the worker result.
pub(crate) fn current_id() -> usize {
    HANDLES.with(|h| {
        h.borrow()
            .as_ref()
            .expect("runtime handles not installed")
            .current_id
            .get()
            .expect("current_id not set")
    })
}

/// Return the earliest deadline in the wheel, if any.
///
/// The executor uses this to compute its park timeout: `deadline - now`.
/// When there are no pending timers the executor can block for a fixed
/// fallback timeout or, once `mio::Poll` is wired in, block forever waiting
/// for I/O.
pub(crate) fn next_deadline(wheel: &TimerWheel) -> Option<Instant> {
    wheel.borrow().peek().map(|r| r.0.0)
}

/// Pop every entry whose deadline has elapsed and return their task ids.
///
/// The executor calls this after parking, then enqueues the returned ids.
/// The tasks are re-polled and their `Sleep` futures see `now >= deadline`
/// and return `Ready`.
pub(crate) fn expire_due(wheel: &TimerWheel) -> Vec<usize> {
    let now = Instant::now();
    let mut ids = Vec::new();
    loop {
        let before = !matches!(
            wheel.borrow().peek(),
            Some(Reverse((deadline, _))) if *deadline <= now
        );
        if before {
            break;
        }
        if let Some(Reverse((_, id))) = wheel.borrow_mut().pop() {
            ids.push(id);
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Runtime;
    use crate::runtime_state::RuntimeState;
    use crate::waker::create_waker;
    use std::cell::Cell;
    use std::task::Context;

    // The timer wheel is a min-heap: `BinaryHeap` is a max-heap by default, so
    // wrapping each entry in `Reverse` inverts the ordering. The `BinaryHeap`
    // sees the *smallest* `Reverse` value at the top, which means the entry with
    // the *earliest* deadline rises to the top. This test proves the ordering:
    // insert 100ms, 50ms, 150ms in any order, and the heap pops 50ms first.
    #[test]
    fn wheel_is_min_heap_by_deadline() {
        let now = Instant::now();
        let mut wheel: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::new();
        wheel.push(Reverse((now + Duration::from_millis(100), 1)));
        wheel.push(Reverse((now + Duration::from_millis(50), 2)));
        wheel.push(Reverse((now + Duration::from_millis(150), 3)));

        assert_eq!(wheel.pop().unwrap().0.1, 2); // 50ms — earliest
        assert_eq!(wheel.pop().unwrap().0.1, 1); // 100ms
        assert_eq!(wheel.pop().unwrap().0.1, 3); // 150ms — latest
        assert!(wheel.is_empty());
    }

    // `sleep(Duration::ZERO)` completes immediately without ever parking: the
    // first poll sees the deadline is already past and returns `Ready`. This is
    // the simplest integration test that proves the `Sleep` future is wired
    // into the executor correctly.
    #[test]
    fn sleep_zero_completes_in_runtime() {
        let mut runtime = Runtime::new();
        let flag = Rc::new(Cell::new(false));
        {
            let flag = flag.clone();
            runtime.spawn(async move {
                sleep(Duration::ZERO).await;
                flag.set(true);
            });
        }
        runtime.run().expect("run should not fail");
        assert!(flag.get());
    }

    // Two concurrent `sleep(100ms)` tasks must finish in less than 200ms wall
    // time — if the executor parks with the earliest deadline (not a fixed
    // 100ms), both tasks wake together.  This proves the executor computes its
    // park timeout from the timer wheel rather than sleeping for a fixed
    // duration or serialising the tasks.
    #[test]
    fn concurrent_sleeps_run_in_parallel() {
        let mut runtime = Runtime::new();
        let counter = Rc::new(Cell::new(0usize));
        for _ in 0..2 {
            let c = counter.clone();
            runtime.spawn(async move {
                sleep(Duration::from_millis(100)).await;
                c.set(c.get() + 1);
            });
        }

        let start = Instant::now();
        runtime.run().expect("run should not fail");
        let elapsed = start.elapsed();

        assert_eq!(counter.get(), 2);
        // Must not be serial: 2 × 100ms = 200ms.  Allow generous headroom.
        assert!(elapsed < Duration::from_millis(125));
    }

    // `yield_now()` returns `Pending` on its first poll, self-wakes via
    // `wake_by_ref`, and returns `Ready` on the second poll.  Combined with
    // `.await`, the task suspends exactly once and resumes on the next
    // iteration of the drain loop.  A counter set *after* the `yield_now().await`
    // proves the task was polled twice (first to suspend, second to complete).
    #[test]
    fn yield_now_returns_pending_then_ready() {
        let mut runtime = Runtime::new();
        let flag = Rc::new(Cell::new(false));
        {
            let flag = flag.clone();
            runtime.spawn(async move {
                yield_now().await;
                flag.set(true);
            });
        }
        runtime.run().expect("run should not fail");
        assert!(flag.get());
    }

    // The "cancellation trap": when a `Sleep` future
    // is dropped before its deadline, its `Drop` impl must remove the
    // `(deadline, id)` entry from the shared wheel.  Otherwise a stale entry
    // blocks the termination check (`wheel.is_empty()`) forever.  This test
    // creates a `Sleep` directly, polls it once (inserting the entry), drops
    // it, and asserts the wheel is empty.
    #[test]
    fn dropped_sleep_removes_itself_from_wheel() {
        let state = RuntimeState::new();
        let wheel: TimerWheel = Rc::new(RefCell::new(BinaryHeap::new()));
        let waker = create_waker(state.clone(), 1);
        let mut cx = Context::from_waker(&waker);

        let mut sleep_val = Sleep {
            wheel: wheel.clone(),
            deadline: Instant::now() + Duration::from_secs(1000),
            id: 1,
            waker: None,
            done: false,
        };

        // First poll pushes (deadline, 1) into the wheel.
        assert!(Pin::new(&mut sleep_val).poll(&mut cx).is_pending());
        assert_eq!(wheel.borrow().peek().unwrap().0.1, 1);

        // Drop the Sleep — the Drop impl filters the entry out.
        drop(sleep_val);
        assert!(wheel.borrow().is_empty());
    }
}
