use std::cell::{Cell, RefCell};
use std::task::Waker;
use std::time::Instant;

/// An unsorted list of `(deadline, id, Waker)` entries. `next_deadline` scans
/// the list for the earliest deadline, and `expire_due` walks the whole list
/// to fire timers whose deadline has passed. The waker is stored inside the
/// entry so that `expire_due` can wake tasks directly — the waker does the
/// work.
pub(crate) struct TimerRegistry {
    entries: RefCell<Vec<TimerEntry>>,
    next_id: Cell<usize>,
}

struct TimerEntry {
    deadline: Instant,
    id: usize,
    waker: Waker,
}

impl TimerRegistry {
    pub(crate) fn new() -> Self {
        TimerRegistry {
            entries: RefCell::new(Vec::new()),
            next_id: Cell::new(0),
        }
    }

    /// Push a timer entry and return its id (used for Drop cleanup).
    pub(crate) fn push(&self, deadline: Instant, waker: Waker) -> usize {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        self.entries.borrow_mut().push(TimerEntry {
            deadline,
            id,
            waker,
        });
        id
    }

    /// Remove an entry by id (used by `Sleep::Drop` for cancellation).
    pub(crate) fn remove(&self, target_id: usize) {
        self.entries
            .borrow_mut()
            .retain(|entry| entry.id != target_id);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    /// Return the earliest deadline in the registry, if any.
    ///
    /// The executor uses this to compute its poll timeout: `deadline - now`.
    /// Scans the full list to find the minimum deadline.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries.borrow().iter().map(|e| e.deadline).min()
    }

    /// Walk the entire list, collecting wakers for every entry whose deadline
    /// has elapsed, and waking each one.
    ///
    /// The executor calls this after polling. The waker's `wake_by_ref()` pushes
    /// the task's id back onto the ready queue, so the task gets re-polled and
    /// its `Sleep` future sees `now >= deadline` and returns `Ready`.
    pub(crate) fn expire_due(&self) {
        let now = Instant::now();
        let mut entries = self.entries.borrow_mut();
        let mut due = Vec::new();
        entries.retain(|e| {
            if e.deadline <= now {
                due.push(e.waker.clone());
                false
            } else {
                true
            }
        });
        drop(entries);
        for w in due {
            w.wake_by_ref();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_state::TaskId;
    use crate::waker::create_waker;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // `next_deadline` scans the list for the earliest deadline. Push durations
    // in arbitrary order and verify the scan returns the minimum.
    #[test]
    fn next_deadline_finds_earliest() {
        let now = Instant::now();
        let heap = TimerRegistry::new();
        heap.push(now + Duration::from_millis(100), Waker::noop().clone());
        heap.push(now + Duration::from_millis(50), Waker::noop().clone());
        heap.push(now + Duration::from_millis(150), Waker::noop().clone());

        assert_eq!(
            heap.next_deadline().unwrap(),
            now + Duration::from_millis(50)
        );
    }

    // `expire_due` fires only timers whose deadline has passed. Push one entry
    // in the past and one in the far future; after calling `expire_due`, the
    // past waker lands in the queue and the future entry remains.
    #[test]
    fn expire_due_wakes_only_expired_entries() {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let heap = TimerRegistry::new();

        let waker_past = create_waker(queue.clone(), TaskId(10));
        let waker_future = create_waker(queue.clone(), TaskId(20));

        heap.push(Instant::now() - Duration::from_secs(1), waker_past);
        heap.push(Instant::now() + Duration::from_secs(1000), waker_future);

        heap.expire_due();

        assert_eq!(*queue.lock().unwrap(), VecDeque::from([TaskId(10)]));
        assert!(!heap.is_empty());
        assert!(heap.next_deadline().is_some());
    }
}
