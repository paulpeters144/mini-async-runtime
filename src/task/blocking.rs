use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};

use crate::context;
use crate::runtime_state::RuntimeState;
use crate::task::worker_pool::Job;

// ---------------------------------------------------------------------------
// BlockingTask — a future that offloads a closure to a worker thread and
// wakes the executor when the result is ready.
// ---------------------------------------------------------------------------

pub struct BlockingTask<R> {
    state: std::rc::Rc<std::cell::RefCell<RuntimeState>>,
    id: usize,
    rx: Option<mpsc::Receiver<std::thread::Result<R>>>,
    registered: bool,
    done: bool,
}

#[cfg(test)]
impl<R> BlockingTask<R> {
    pub(crate) fn id(&self) -> usize {
        self.id
    }
}

impl<R> Future for BlockingTask<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        let this = self.get_mut();
        if this.done {
            unreachable!("BlockingTask polled after completion");
        }

        if !this.registered {
            this.state
                .borrow_mut()
                .blocking_wakers
                .insert(this.id, cx.waker().clone());
            this.registered = true;
        } else if let Some(existing) = this.state.borrow_mut().blocking_wakers.get_mut(&this.id) {
            *existing = cx.waker().clone();
        }

        match this.rx.as_mut().unwrap().try_recv() {
            Ok(Ok(result)) => {
                this.done = true;
                this.state.borrow_mut().blocking_wakers.remove(&this.id);
                Poll::Ready(result)
            }
            // The closure panicked on the worker thread. The payload crossed
            // the result channel, so rethrow it here — on the executor thread,
            // inside the waiting task's poll — so `run()` panics with the
            // original payload instead of hanging on a result that never comes.
            Ok(Err(payload)) => {
                this.done = true;
                this.state.borrow_mut().blocking_wakers.remove(&this.id);
                std::panic::resume_unwind(payload);
            }
            Err(mpsc::TryRecvError::Empty) => Poll::Pending,
            Err(mpsc::TryRecvError::Disconnected) => {
                panic!("worker thread disconnected without sending a result");
            }
        }
    }
}

impl<R> Drop for BlockingTask<R> {
    fn drop(&mut self) {
        if !self.done {
            self.state.borrow_mut().blocking_wakers.remove(&self.id);
        }
    }
}

pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (state, job_tx) = context::with(|ctx| {
        (ctx.state.clone(), ctx.job_tx.clone())
    });

    let task_id = {
        let mut state = state.borrow_mut();
        let id = state.next_blocking_id;
        state.next_blocking_id += 1;
        id
    };

    // The result channel carries the closure's outcome — value OR panic
    // payload — so a panicking closure never vanishes silently: the payload
    // is resumed on the executor thread when the BlockingTask is re-polled.
    let (tx, rx) = mpsc::channel::<std::thread::Result<R>>();
    let job: Job = Box::new(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let _ = tx.send(result);
    });
    job_tx.send(job).expect("worker pool is shut down");
    BlockingTask {
        state,
        id: task_id,
        rx: Some(rx),
        registered: false,
        done: false,
    }
}
