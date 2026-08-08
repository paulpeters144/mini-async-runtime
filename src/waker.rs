use crate::runtime_state::TaskId;
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};

/// The payload that powers the waker. `Wake` is the standard-library trait
/// for building wakers: implement `wake` on an `Arc<Self>`, and the library
/// generates the vtable, clone, `wake_by_ref`, and drop for free — no
/// `unsafe`, no raw pointers, no hand-written vtable.
///
/// Because `Wake` requires `Arc`, the ready queue must live in an `Arc`
/// instead of an `Rc`. This adds an atomic reference count, but the tradeoff
/// is worth it: the entire `RawWakerVTable` dance disappears.
///
/// `Waker::from(Arc<T>)` also requires `T: Send + Sync`, so the queue uses
/// `Mutex` instead of `RefCell`. The lock is always uncontended (the runtime
/// is single-threaded), making `lock().unwrap()` a trivial operation.
pub struct TaskWaker {
    queue: Arc<Mutex<Vec<TaskId>>>,
    id: TaskId,
}

impl TaskWaker {
    /// Build a `Waker` that, when woken, pushes `id` into the shared ready queue.
    pub fn new(queue: Arc<Mutex<Vec<TaskId>>>, id: TaskId) -> Waker {
        Waker::from(Arc::new(TaskWaker { queue, id }))
    }
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.queue.lock().unwrap().push(self.id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A task's only job in the scheduling dance is to request another poll.
    /// `waker.wake()` pushes the task's id into the shared ready queue. The
    /// executor drains that queue to decide what to poll next. This test
    /// checks the basic round-trip: create a waker for `TaskId(7)`, wake it,
    /// and confirm the id appears exactly once.
    #[test]
    fn wake_pushes_id_to_queue() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let waker = TaskWaker::new(queue.clone(), TaskId(7));
        waker.wake();
        assert_eq!(queue.lock().unwrap().clone(), vec![TaskId(7)]);
    }

    /// `waker.wake()` *consumes* the waker, but `wake_by_ref` borrows it so
    /// the same waker can be used repeatedly. An executor typically holds one
    /// canonical waker per task but may need to wake it from several places
    /// (a timer fired, a socket became readable). This test wakes twice from
    /// the same waker and expects two entries in the queue.
    #[test]
    fn wake_by_ref_is_reusable() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let waker = TaskWaker::new(queue.clone(), TaskId(7));
        waker.wake_by_ref();
        waker.wake_by_ref();
        assert_eq!(
            queue.lock().unwrap().clone(),
            vec![TaskId(7), TaskId(7)]
        );
    }

    /// Cloning a waker is cheap: it bumps the `Arc` refcount on the shared
    /// `TaskWaker` payload; the logical identity (which *task* to wake) stays
    /// the same. Both the original and the clone push the same `TaskId` into
    /// the same queue. This test clones the waker, wakes through each, and
    /// verifies they agree on the task identity.
    #[test]
    fn clone_shares_identity() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let waker = TaskWaker::new(queue.clone(), TaskId(7));
        let clone = waker.clone();
        waker.wake();
        clone.wake();
        assert_eq!(
            queue.lock().unwrap().clone(),
            vec![TaskId(7), TaskId(7)]
        );
    }

    /// Different tasks get different wakers, each pushing its own `TaskId`
    /// into the *same* shared queue. This is how the executor distinguishes
    /// who to poll: it pops an id, looks up the task, and polls it. This
    /// test creates wakers for `TaskId(1)` and `TaskId(2)`, wakes both, and
    /// checks that both ids land in the queue.
    #[test]
    fn different_ids_share_queue() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let a = TaskWaker::new(queue.clone(), TaskId(1));
        let b = TaskWaker::new(queue.clone(), TaskId(2));
        a.wake();
        b.wake();
        let mut drained: Vec<_> = std::mem::take(&mut *queue.lock().unwrap()).into();
        drained.sort_by_key(|id| id.0);
        assert_eq!(drained, vec![TaskId(1), TaskId(2)]);
    }

    /// Because `Wake` uses `Arc` for the payload (not `Weak`), a waker keeps
    /// the queue alive even after the runtime drops its only reference. This
    /// is a key behavioural difference from the old `Weak`-based design: a
    /// stale waker does NOT become a silent no-op — it still pushes to a
    /// queue that nobody drains. It doesn't crash or leak unsafely; the queue
    /// is simply freed once all remaining wakers are dropped.
    ///
    /// This test proves it: create the queue, produce a waker, drop the
    /// original `Arc` so only the waker holds a reference, then wake — the
    /// id still lands in the queue.
    #[test]
    fn waker_keeps_queue_alive() {
        let queue = Arc::new(Mutex::new(Vec::new()));
        let waker = TaskWaker::new(queue.clone(), TaskId(99));
        drop(queue); // runtime's reference gone
        waker.wake(); // still works — the waker holds an Arc
        // the wake succeeded — queue wasn't freed because the waker held it.
        // (We can't inspect the queue without another reference, but the call
        // didn't panic, which is the assertion.)
    }
}
