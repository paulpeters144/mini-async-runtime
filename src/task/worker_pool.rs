use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

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
    #[allow(clippy::needless_pass_by_value)]
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
                            // panics and ship the payload; this guard is
                            // defense-in-depth. Either way: wake and keep
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

    /// Return a clone of the job sender — used by `spawn_blocking` (via the
    /// thread-local context) and by the pool's own tests.
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

#[cfg(test)]
mod tests {
    use super::*;
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
        pool.job_tx()
            .send(Box::new(move || {
                c.store(42, Ordering::SeqCst);
            }))
            .unwrap();

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
            pool.job_tx()
                .send(Box::new(move || {
                    b.wait();
                    seq.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(100));
                }))
                .unwrap();
        }
        {
            let seq = seq.clone();
            let b = barrier.clone();
            pool.job_tx()
                .send(Box::new(move || {
                    b.wait();
                    seq.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(100));
                }))
                .unwrap();
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

    // Jobs sent before drop still run before the worker exits.
    #[test]
    fn jobs_sent_before_drop_complete() {
        let pool = WorkerPool::new(make_waker());
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let c = counter.clone();
            pool.job_tx()
                .send(Box::new(move || {
                    c.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
        }

        drop(pool);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
