pub mod blocking;
pub mod yield_now;
pub(crate) mod worker_pool;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(test)]
use std::task::Waker;

pub struct Task {
    id: usize,
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl Task {
    pub fn new(id: usize, f: impl Future<Output = ()> + 'static) -> Self {
        let future = Box::pin(f);
        Task { id, future }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

pub use blocking::spawn_blocking;
pub use yield_now::yield_now;

// A `Task` is the unit of work the executor owns: an id the waker uses to
// reach it, plus the future itself. They check the two promises a task makes
// to the executor: I can be polled" and "I remember where I left off between
// polls".

// Why `Box::pin`? Two separate problems solved at once.
//   - Box: the future lives on the heap, so a `Task` is always a small, fixed
//     size: an id plus one fat pointer (the trait object's data pointer and
//     vtable pointer). A future that captures a megabyte still fits in a Task.
//   - Pin: once polled, a future may hold pointers into its own memory (that
//     is how `async fn` state machines work internally). If the Task moved,
//     those self-pointers would dangle. Pinning makes the future immovable, so
//     it is safe to come back and poll it again later.
// Here we build a deliberately huge future and show the Task stays small.
#[test]
fn new_wraps_future_with_id() {
    struct HugeFuture([u8; 64 * 1024]);

    impl Future for HugeFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let _ = &self.0;
            Poll::Ready(())
        }
    }

    let task = Task::new(7, HugeFuture([0; 64 * 1024]));

    assert_eq!(task.id(), 7);

    // A `Task` holds only an id and the future. The future is a trait object
    // behind a `Box`, and a trait object is a *fat* pointer: a data pointer
    // plus a vtable pointer. So the size adds up as:
    let word = std::mem::size_of::<usize>();
    let data_ptr = word; // the Box's pointer to the heap allocation
    let vtable_ptr = word; // the vtable needed to call `Future::poll`
    let id = word; // the task's id
    let expected = data_ptr + vtable_ptr + id;
    assert_eq!(std::mem::size_of::<Task>(), expected);
}

// A completed future: `async {}` has no awaits, so the first poll is also the
// last — it reports Ready immediately. This is the simplest possible task:
// nothing to wait for, nothing to re-poll. The executor can drop it right away.
#[test]
fn poll_returns_ready_when_future_completes() {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut task = Task::new(0, async {});

    assert_eq!(task.poll(&mut cx), Poll::Ready(()));
}

// Real futures rarely finish on the first poll. An I/O future (a socket read,
// a timer) returns Pending while it waits, and the executor must come back and
// poll it again after a wake. This test simulates that cycle.
//
// `PollTwice` behaves like a read that is not ready yet: the first poll finds
// the data missing and returns Pending; the "wake" (our second poll) finds it
// arrived. Because the future is stored boxed and pinned, it can safely keep
// its `polled` counter in its own memory across the two polls — nothing moved
// in between, so the counter is still where it was left.
#[cfg(test)]
struct PollTwice {
    polled: usize,
}

#[cfg(test)]
impl Future for PollTwice {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        self.polled += 1;
        if self.polled == 1 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[test]
fn pending_then_ready_resumes_across_polls() {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut task = Task::new(0, PollTwice { polled: 0 });

    // First poll: "not ready yet". The task keeps the future for later.
    assert_eq!(task.poll(&mut cx), Poll::Pending);
    assert_eq!(task.poll(&mut cx), Poll::Ready(()));
}
