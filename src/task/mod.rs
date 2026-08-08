pub mod all;
pub mod blocking;
pub(crate) mod worker_pool;

use crate::runtime_state::TaskId;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

pub struct Task {
    id: TaskId,
    waker: Waker,
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl Task {
    pub fn new(id: TaskId, f: impl Future<Output = ()> + 'static, waker: Waker) -> Self {
        let future = Box::pin(f);
        Task { id, waker, future }
    }

    #[must_use]
    pub fn id(&self) -> TaskId {
        self.id
    }

    #[must_use]
    pub fn waker(&self) -> &Waker {
        &self.waker
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

pub use all::all;
pub use blocking::spawn_blocking;

// A `Task` is the unit of work the executor owns: an id the waker uses to
// reach it, plus the waker itself and the future. They check the two promises a
// task makes to the executor: "I can be polled" and "I remember where I left off
// between polls".

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
    struct HugeFuture(Box<[u8; 64 * 1024]>);

    impl Future for HugeFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            let _ = &self.0;
            Poll::Ready(())
        }
    }

    #[allow(clippy::large_stack_arrays)]
    let huge_data = Box::new([0; 64 * 1024]);
    let task = Task::new(TaskId(7), HugeFuture(huge_data), Waker::noop().clone());

    assert_eq!(task.id(), TaskId(7));

    // A `Task` holds an id, a waker, and the future. The future is a trait object
    // behind a `Box`, and a trait object is a *fat* pointer: a data pointer
    // plus a vtable pointer. A `Waker` is a *fat* pointer too (RawWaker carries
    // a data pointer and a vtable pointer). So the size adds up as:
    let word = std::mem::size_of::<usize>();
    let data_ptr = word;
    let vtable_ptr = word;
    let id = word;
    let waker_ptr = word;
    let waker_vptr = word;
    let expected = data_ptr + vtable_ptr + id + waker_ptr + waker_vptr;
    assert_eq!(std::mem::size_of::<Task>(), expected);
}

// A completed future: `async {}` has no awaits, so the first poll is also the
// last — it reports Ready immediately. This is the simplest possible task:
// nothing to wait for, nothing to re-poll. The executor can drop it right away.
#[test]
fn poll_returns_ready_when_future_completes() {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut task = Task::new(TaskId(0), async {}, Waker::noop().clone());

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
    let mut task = Task::new(TaskId(0), PollTwice { polled: 0 }, Waker::noop().clone());

    // First poll: "not ready yet". The task keeps the future for later.
    assert_eq!(task.poll(&mut cx), Poll::Pending);
    assert_eq!(task.poll(&mut cx), Poll::Ready(()));
}
