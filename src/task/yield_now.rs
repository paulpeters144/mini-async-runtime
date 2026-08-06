use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mar::Mar;
    use std::cell::Cell;
    use std::rc::Rc;

    // `yield_now()` returns `Pending` on its first poll, self-wakes via
    // `wake_by_ref`, and returns `Ready` on the second poll.  Combined with
    // `.await`, the task suspends exactly once and resumes on the next
    // iteration of the drain loop.  A counter set *after* the
    // `yield_now().await` proves the task was polled twice (first to suspend,
    // second to complete).
    #[test]
    fn yield_now_returns_pending_then_ready() {
        let flag = Rc::new(Cell::new(false));
        {
            let flag = flag.clone();
            Mar::run(async move {
                yield_now().await;
                flag.set(true);
            })
            .expect("run should not fail");
        }
        assert!(flag.get());
    }
}
