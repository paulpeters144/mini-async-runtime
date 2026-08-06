use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};

use crate::runtime_state::RuntimeState;
use crate::timer_wheel;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A fixed-size pool of worker threads that run blocking closures off the
/// executor thread. Each worker holds a clone of the shared `mio::Waker` and
/// calls `wake()` after every job, which lets the executor know a result is
/// ready.
pub struct WorkerPool {
    job_tx: Option<mpsc::Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Create a pool with one worker thread.
    pub fn new(waker: Arc<mio::Waker>) -> Self {
        Self::with_count(waker, 1)
    }

    /// Create a pool with `n` worker threads.
    pub fn with_count(waker: Arc<mio::Waker>, n: usize) -> Self {
        let (job_tx, job_rx) = mpsc::channel::<Job>();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let mut workers = Vec::with_capacity(n);

        for _ in 0..n {
            let rx = job_rx.clone();
            let w = waker.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = rx.lock().unwrap().recv();
                    match job {
                        Ok(job) => {
                            // A panicking job must not kill the worker — a dead
                            // worker would silently strand every future job.
                            // `spawn_blocking` jobs already catch their own
                            // panics and ship the payload; this guard covers
                            // raw `submit()` jobs. Either way: wake and keep
                            // serving.
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                            let _ = w.wake();
                        }
                        Err(_) => break,
                    }
                }
            }));
        }

        WorkerPool {
            job_tx: Some(job_tx),
            workers,
        }
    }

    /// Send a closure to a worker thread. The closure runs once and its result
    /// is discarded — the caller should arrange its own back-channel for the
    /// return value (typically an `mpsc::channel` inside `spawn_blocking`).
    pub fn submit(&self, job: Job) {
        self.job_tx
            .as_ref()
            .expect("worker pool shut down")
            .send(job)
            .expect("worker thread panicked");
    }

    /// Return a clone of the job sender — used by `blocking::install()` to
    /// give the thread-local handle a way to submit work.
    pub(crate) fn job_tx(&self) -> mpsc::Sender<Job> {
        self.job_tx.as_ref().expect("worker pool shut down").clone()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Drop the sender first to close the channel; workers exit their
        // recv() loop when the channel disconnects.
        drop(self.job_tx.take());
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Thread-local bridge: lets `spawn_blocking()` talk to the worker pool and
// shared state without explicit handles (same pattern as `timer_wheel::sleep`).
// ---------------------------------------------------------------------------

thread_local! {
    static BLOCKING_HANDLE: std::cell::RefCell<Option<BlockingHandle>> =
        const { std::cell::RefCell::new(None) };
}

struct BlockingHandle {
    state: std::rc::Rc<std::cell::RefCell<RuntimeState>>,
    job_tx: mpsc::Sender<Job>,
}

pub(crate) fn install(
    state: std::rc::Rc<std::cell::RefCell<RuntimeState>>,
    job_tx: mpsc::Sender<Job>,
) {
    BLOCKING_HANDLE.with(|h| {
        *h.borrow_mut() = Some(BlockingHandle { state, job_tx });
    });
}

pub(crate) fn uninstall() {
    BLOCKING_HANDLE.with(|h| {
        *h.borrow_mut() = None;
    });
}

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
    /// Test-only handles for the Phase 3 Drop test.
    pub(crate) fn state(&self) -> std::rc::Rc<std::cell::RefCell<RuntimeState>> {
        self.state.clone()
    }

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
                .blocking
                .insert(this.id, cx.waker().clone());
            this.registered = true;
        } else if let Some(existing) = this.state.borrow_mut().blocking.get_mut(&this.id) {
            *existing = cx.waker().clone();
        }

        match this.rx.as_mut().unwrap().try_recv() {
            Ok(Ok(result)) => {
                this.done = true;
                this.state.borrow_mut().blocking.remove(&this.id);
                Poll::Ready(result)
            }
            // The closure panicked on the worker thread. The payload crossed
            // the result channel, so rethrow it here — on the executor thread,
            // inside the waiting task's poll — so `run()` panics with the
            // original payload instead of hanging on a result that never comes.
            Ok(Err(payload)) => {
                this.done = true;
                this.state.borrow_mut().blocking.remove(&this.id);
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
            self.state.borrow_mut().blocking.remove(&self.id);
        }
    }
}

pub fn spawn_blocking<F, R>(f: F) -> BlockingTask<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (handle, task_id) = BLOCKING_HANDLE.with(|h| {
        let h = h.borrow();
        let h = h
            .as_ref()
            .expect("spawn_blocking() called outside of runtime");
        let state = h.state.clone();
        let job_tx = h.job_tx.clone();
        let task_id = timer_wheel::current_id();
        ((state, job_tx), task_id)
    });

    // The result channel carries the closure's outcome — value OR panic
    // payload — so a panicking closure never vanishes silently: the payload
    // is resumed on the executor thread when the BlockingTask is re-polled.
    let (tx, rx) = mpsc::channel::<std::thread::Result<R>>();
    let job: Job = Box::new(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let _ = tx.send(result);
    });
    handle.1.send(job).expect("worker pool is shut down");
    BlockingTask {
        state: handle.0,
        id: task_id,
        rx: Some(rx),
        registered: false,
        done: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn make_waker() -> Arc<mio::Waker> {
        let poll = mio::Poll::new().unwrap();
        Arc::new(mio::Waker::new(poll.registry(), mio::Token(7)).unwrap())
    }

    // A closure sent to a worker runs and produces an observable side-effect.
    #[test]
    fn worker_runs_a_closure() {
        let counter = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::new(make_waker());

        let c = counter.clone();
        pool.submit(Box::new(move || {
            c.store(42, Ordering::SeqCst);
        }));

        // The job runs asynchronously.  Drop the pool to wait for the worker
        // to finish, then read the counter.
        drop(pool);

        assert_eq!(counter.load(Ordering::SeqCst), 42);
    }

    // Two workers pick up two jobs and run them concurrently. The total wall
    // time is roughly the max job duration, not the sum (serial would be sum).
    #[test]
    fn two_workers_run_jobs_concurrently() {
        let pool = WorkerPool::with_count(make_waker(), 2);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let seq = Arc::new(AtomicUsize::new(0));
        let start = std::time::Instant::now();

        {
            let seq = seq.clone();
            let b = barrier.clone();
            pool.submit(Box::new(move || {
                b.wait();
                seq.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(100));
            }));
        }
        {
            let seq = seq.clone();
            let b = barrier.clone();
            pool.submit(Box::new(move || {
                b.wait();
                seq.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(100));
            }));
        }

        // The pool is consumed to join the workers.
        drop(pool);

        let elapsed = start.elapsed();
        assert_eq!(seq.load(Ordering::SeqCst), 2);
        // Serial execution would take ~200ms; concurrent < 150ms.
        assert!(elapsed < Duration::from_millis(150));
    }

    // Dropping the pool closes the job channel; every worker exits its loop
    // and the JoinHandles resolve.
    #[test]
    fn workers_exit_when_pool_is_dropped() {
        let pool = WorkerPool::new(make_waker());
        // If we got here without deadlocking, the workers exited.
        drop(pool);
    }

    // Jobs submitted before drop still run before the worker exits.
    #[test]
    fn jobs_submitted_before_drop_complete() {
        let pool = WorkerPool::new(make_waker());
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let c = counter.clone();
            pool.submit(Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }

        drop(pool);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
