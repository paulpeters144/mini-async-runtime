use super::timer_registry::TimerRegistry;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// A future that completes after a given duration has elapsed.
///
/// On the **first** poll it pushes `(deadline, id, Waker)` into the shared
/// registry (getting a fresh id from the registry) and returns `Pending`.  On
/// a later poll (after the executor expires the deadline and wakes the task)
/// it sees `Instant::now() >= deadline` and returns `Ready`.  If the deadline
/// has already passed by the first poll (e.g. `sleep(0ms)`), it returns
/// `Ready` immediately without touching the registry.
///
/// **Cancellation trap:** if a `Sleep` is dropped before the deadline fires,
/// its `Drop` impl removes its entry from the registry so the executor's
/// termination check (`registry.is_empty()`) is not blocked by a stale entry.
pub struct Sleep {
    registry: Rc<TimerRegistry>,
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
            let id = this.registry.push(this.deadline, cx.waker().clone());
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
            self.registry.remove(id);
        }
    }
}

/// Pause the current task for at least `duration`.
///
/// Reads the shared timer registry from the runtime context installed by
/// `Mar::run()`.  Calling it outside `run()` panics with a clear message.
///
/// `sleep(Duration::ZERO)` is valid and completes immediately on the first
/// poll without ever entering the registry.
pub fn sleep(duration: Duration) -> Sleep {
    let registry = crate::context::with(|ctx| ctx.wheel.clone());
    Sleep {
        registry,
        deadline: Instant::now() + duration,
        id: None,
        done: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mar::Mar;
    use crate::runtime_state::{RuntimeState, TaskId};
    use crate::waker::TaskWaker;
    use std::cell::Cell;
    use std::task::Context;

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
    // from the shared registry.  Otherwise a stale entry blocks the
    // termination check (`registry.is_empty()`) forever.  This test creates a
    // `Sleep` directly, polls it once (inserting the entry), drops it, and
    // asserts the registry is empty.
    #[test]
    fn dropped_sleep_removes_itself_from_heap() {
        let state = RuntimeState::new();
        let heap = Rc::new(TimerRegistry::new());
        let waker = TaskWaker::new(state.borrow().queue.clone(), TaskId(1));
        let mut cx = Context::from_waker(&waker);

        let mut sleep_val = Sleep {
            registry: heap.clone(),
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
