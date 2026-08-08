use std::cell::{Cell, RefCell};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// A priority queue of `(deadline, id, Waker)`, ordered so the entry with the
/// **earliest** deadline is always at the top.
///
/// `BinaryHeap` is a max-heap by default — it keeps the *largest* element at
/// the top. Wrapping every entry in `Reverse` flips the ordering: the
/// *earliest* `deadline` rises to the top. The waker is stored inside the heap
/// entry so that `expire_due` can wake tasks directly without needing to know
/// their id — the waker does the work.
pub(crate) struct TimerHeap {
    heap: RefCell<BinaryHeap<Reverse<TimerEntry>>>,
    next_id: Cell<usize>,
}

struct TimerEntry {
    deadline: Instant,
    id: usize,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

impl TimerHeap {
    pub(crate) fn new() -> Self {
        TimerHeap {
            heap: RefCell::new(BinaryHeap::new()),
            next_id: Cell::new(0),
        }
    }

    /// Push a timer entry and return its id (used for Drop cleanup).
    pub(crate) fn push(&self, deadline: Instant, waker: Waker) -> usize {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        self.heap.borrow_mut().push(Reverse(TimerEntry {
            deadline,
            id,
            waker,
        }));
        id
    }

    /// Remove an entry by id (used by `Sleep::Drop` for cancellation).
    pub(crate) fn remove(&self, target_id: usize) {
        self.heap
            .borrow_mut()
            .retain(|Reverse(entry)| entry.id != target_id);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.heap.borrow().is_empty()
    }
}

/// A future that completes after a given duration has elapsed.
///
/// On the **first** poll it pushes `(deadline, id, Waker)` into the shared heap
/// (getting a fresh id from the heap) and returns `Pending`.  On a later poll
/// (after the executor expires the deadline and wakes the task) it sees
/// `Instant::now() >= deadline` and returns `Ready`.  If the deadline has
/// already passed by the first poll (e.g. `sleep(0ms)`), it returns `Ready`
/// immediately without touching the heap.
///
/// **Cancellation trap:** if a `Sleep` is dropped before the deadline fires,
/// its `Drop` impl removes its entry from the heap so the executor's
/// termination check (`heap.is_empty()`) is not blocked by a stale entry.
pub struct Sleep {
    heap: Rc<TimerHeap>,
    deadline: Instant,
    id: Option<usize>,
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

        if this.id.is_none() {
            let id = this.heap.push(this.deadline, cx.waker().clone());
            this.id = Some(id);
        }

        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if !self.done
            && let Some(id) = self.id
        {
            self.heap.remove(id);
        }
    }
}

/// Pause the current task for at least `duration`.
///
/// Reads the shared timer heap from the runtime context installed by
/// `Mar::run()`.  Calling it outside `run()` panics with a clear message.
///
/// `sleep(Duration::ZERO)` is valid and completes immediately on the first
/// poll without ever entering the heap.
pub fn sleep(duration: Duration) -> Sleep {
    let heap = crate::context::with(|ctx| ctx.wheel.clone());
    Sleep {
        heap,
        deadline: Instant::now() + duration,
        id: None,
        done: false,
    }
}

/// Return the earliest deadline in the heap, if any.
///
/// The executor uses this to compute its poll timeout: `deadline - now`.
/// When there are no pending timers the executor can block for a fixed
/// fallback timeout or, once `mio::Poll` is wired in, block forever waiting
/// for I/O.
pub(crate) fn next_deadline(heap: &TimerHeap) -> Option<Instant> {
    heap.heap.borrow().peek().map(|r| r.0.deadline)
}

/// Pop every timer entry whose deadline has elapsed and wake the
/// corresponding task via its stored waker.
///
/// The executor calls this after polling. The waker's `wake_by_ref()` pushes
/// the task's id back onto the ready queue, so the task gets re-polled and
/// its `Sleep` future sees `now >= deadline` and returns `Ready`.
pub(crate) fn expire_due(heap: &TimerHeap) {
    let now = Instant::now();
    loop {
        let should_pop = heap
            .heap
            .borrow()
            .peek()
            .is_some_and(|r| r.0.deadline <= now);
        if !should_pop {
            break;
        }
        if let Some(Reverse(entry)) = heap.heap.borrow_mut().pop() {
            entry.waker.wake_by_ref();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mar::Mar;
    use crate::runtime_state::{RuntimeState, TaskId};
    use crate::waker::create_waker;
    use std::cell::Cell;
    use std::task::Context;

    // The timer heap is a min-heap: `BinaryHeap` is a max-heap by default, so
    // wrapping each entry in `Reverse` inverts the ordering. The `BinaryHeap`
    // sees the *smallest* `Reverse` value at the top, which means the entry with
    // the *earliest* deadline rises to the top. This test proves the ordering:
    // insert 100ms, 50ms, 150ms in any order, and the heap pops 50ms first.
    #[test]
    fn heap_is_min_heap_by_deadline() {
        let now = Instant::now();
        let heap = TimerHeap::new();
        heap.push(now + Duration::from_millis(100), Waker::noop().clone());
        heap.push(now + Duration::from_millis(50), Waker::noop().clone());
        heap.push(now + Duration::from_millis(150), Waker::noop().clone());

        let first = heap.heap.borrow_mut().pop().unwrap().0;
        let second = heap.heap.borrow_mut().pop().unwrap().0;
        let third = heap.heap.borrow_mut().pop().unwrap().0;

        assert!(first.deadline <= second.deadline);
        assert!(second.deadline <= third.deadline);
    }

    // `sleep(Duration::ZERO)` completes immediately without ever polling: the
    // first poll sees the deadline is already past and returns `Ready`. This is
    // the simplest integration test that proves the `Sleep` future is wired
    // into the executor correctly.
    #[test]
    fn sleep_zero_completes_in_runtime() {
        let flag = Rc::new(Cell::new(false));
        {
            let flag = flag.clone();
            Mar::run(async move {
                sleep(Duration::ZERO).await;
                flag.set(true);
            })
            .expect("run should not fail");
        }
        assert!(flag.get());
    }

    // The "cancellation trap": when a `Sleep` future is dropped before its
    // deadline, its `Drop` impl must remove the `(deadline, id, Waker)` entry
    // from the shared heap.  Otherwise a stale entry blocks the termination
    // check (`heap.is_empty()`) forever.  This test creates a `Sleep` directly,
    // polls it once (inserting the entry), drops it, and asserts the heap is
    // empty.
    #[test]
    fn dropped_sleep_removes_itself_from_heap() {
        let state = RuntimeState::new();
        let heap = Rc::new(TimerHeap::new());
        let waker = create_waker(state.borrow().queue.clone(), TaskId(1));
        let mut cx = Context::from_waker(&waker);

        let mut sleep_val = Sleep {
            heap: heap.clone(),
            deadline: Instant::now() + Duration::from_secs(1000),
            id: None,
            done: false,
        };

        assert!(Pin::new(&mut sleep_val).poll(&mut cx).is_pending());
        assert!(!heap.is_empty());

        drop(sleep_val);
        assert!(heap.is_empty());
    }
}
