use crate::context;
use crate::reactor::{Reactor, ReactorHandle};
use crate::runtime_state::RuntimeState;
use crate::task::Task;
use crate::task::worker_pool::WorkerPool;
use crate::time::{self, TimerHeap};
use crate::waker::create_waker;
use std::cell::RefCell;
use std::future::Future;
use std::io::{self};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

const WAKEN_TOKEN: mio::Token = mio::Token(0);

fn spawn_root<F>(runtime: &Mar, future: F)
where
    F: Future<Output = ()> + 'static,
{
    let mut state = runtime.state.borrow_mut();
    let id = state.next_id;
    state.next_id.0 += 1;
    let waker = create_waker(state.queue.clone(), id);
    state.tasks.insert(id, Task::new(id, future, waker));
    state.queue.lock().unwrap().push_back(id);
}

fn drain_ready_queue(runtime: &Mar) {
    loop {
        let next = {
            let state = runtime.state.borrow_mut();
            state.queue.lock().unwrap().pop_front()
        };
        let Some(id) = next else {
            break;
        };

        let Some(mut task) = runtime.state.borrow_mut().tasks.remove(&id) else {
            continue;
        };

        let waker = task.waker().clone();
        let mut cx = Context::from_waker(&waker);
        match task.poll(&mut cx) {
            Poll::Pending => {
                runtime.state.borrow_mut().tasks.insert(id, task);
            }
            Poll::Ready(()) => {}
        }
    }
}

fn is_done(runtime: &Mar) -> bool {
    let state = runtime.state.borrow();
    state.tasks.is_empty() && runtime.wheel.is_empty() && state.blocking_wakers.is_empty()
}

fn poll_readiness_events(runtime: &mut Mar) -> io::Result<()> {
    let timeout = compute_timeout(&runtime.wheel);
    runtime.reactor.borrow_mut().poll(&mut runtime.events, timeout)?;
    Ok(())
}

fn compute_timeout(wheel: &TimerHeap) -> Option<Duration> {
    time::next_deadline(wheel)
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

fn fire_due_timers(runtime: &Mar) {
    time::expire_due(&runtime.wheel);
}

fn wake_completed_blocking(runtime: &Mar) {
    for event in &runtime.events {
        if event.token() == WAKEN_TOKEN {
            let completed = runtime.pool.drain_completed();
            for completed_id in completed {
                let waker = {
                    let state = runtime.state.borrow();
                    state.blocking_wakers.get(&completed_id).cloned()
                };
                if let Some(w) = waker {
                    w.wake_by_ref();
                }
            }
        }
    }
}

pub struct Mar {
    pub(crate) state: Rc<RefCell<RuntimeState>>,
    pub(crate) wheel: Rc<TimerHeap>,
    pub(crate) reactor: ReactorHandle,
    pub(crate) pool: WorkerPool,
    events: mio::Events,
}

impl Mar {
    pub(crate) fn new() -> Self {
        let state = RuntimeState::new();
        let wheel = Rc::new(TimerHeap::new());
        let reactor = Rc::new(RefCell::new(Reactor::new()));
        let pool = {
            let reactor = reactor.borrow();
            let registry = reactor.registry();
            let waker = mio::Waker::new(registry, WAKEN_TOKEN);
            let waker = Arc::new(waker.unwrap());
            WorkerPool::new(waker)
        };
        let events = mio::Events::with_capacity(64);
        Mar {
            state,
            wheel,
            reactor,
            pool,
            events,
        }
    }

    /// # Errors
    ///
    /// Returns an I/O error if the reactor's OS poll fails.
    pub fn run<F>(future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + 'static,
    {
        let mut runtime = Self::new();

        // The guard uninstalls the thread-local context when it drops, which
        // happens before `runtime` drops — on the happy path and on a panic —
        // so the pool can always join its workers.
        let _context = context::install(context::ContextHandle {
            state: runtime.state.clone(),
            wheel: runtime.wheel.clone(),
            job_tx: runtime.pool.job_tx(),
            completed_tx: runtime.pool.completed_tx(),
        });

        spawn_root(&runtime, future);

        loop {
            drain_ready_queue(&runtime);

            if is_done(&runtime) {
                return Ok(());
            }

            poll_readiness_events(&mut runtime)?;
            fire_due_timers(&runtime);
            wake_completed_blocking(&runtime);
        }
    }
}

#[cfg(test)]
use crate::runtime_state::TaskId;
#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
use std::thread;

// The runtime is the thing that makes futures progress. `new()` is just
// allocation: an empty `RuntimeState` with an empty queue and no task map. The
// future passed to `run()` is inserted as the root task and the executor drains
// it to completion.
#[test]
fn new_runtime_has_empty_queue_and_task_map() {
    let runtime = Mar::new();
    let state = runtime.state.borrow();
    assert!(state.queue.lock().unwrap().is_empty());
    assert!(state.tasks.is_empty());
    assert_eq!(state.next_id, TaskId(0));
}

// An empty future completes on its first poll, so `run(async {})` returns
// immediately with a clean state — no queue entries, no lingering tasks.
#[test]
fn run_returns_immediately_with_empty_future() {
    Mar::run(async {}).expect("run should not fail");
}

// A root future with no awaits completes on the first poll and the executor
// returns with both queue and task map empty — termination is total.
#[test]
fn run_completes_task_and_leaves_empty_state() {
    let polls = Rc::new(Cell::new(0usize));
    {
        let polls = polls.clone();
        Mar::run(async move {
            polls.set(polls.get() + 1);
        })
        .expect("run should not fail");
    }

    assert_eq!(polls.get(), 1);
}

// The heart of the executor: a future is not necessarily done on the first
// poll. `Probe` returns `Pending` until it has been polled `target` times, and
// each `Pending` poll re-wakes itself with `wake_by_ref()`. That wake pushes
// its id back onto the queue *during* the drain, so the executor's loop keeps
// running and re-polls it. When `Probe` finally returns `Ready`, the task is
// dropped and the map empties, so `run()` can return. This is the golden-rule
// test that proves the executor re-polls a `Pending` task after a
// wake, which is the whole point of a waker-driven loop.
#[cfg(test)]
struct Probe {
    target: usize,
    polls: Rc<Cell<usize>>,
}

#[cfg(test)]
impl Future for Probe {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        if self.polls.get() >= self.target {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[test]
fn probe_future_re_polls_until_ready() {
    let polls = Rc::new(Cell::new(0usize));
    Mar::run(Probe {
        target: 3,
        polls: polls.clone(),
    })
    .expect("run should not fail");

    assert_eq!(polls.get(), 3);
}

// Smoke test: spawn_blocking called and awaited inside run().
#[test]
fn spawn_blocking_smoke() {
    let done = Rc::new(Cell::new(false));
    {
        let done = done.clone();
        Mar::run(async move {
            let () = crate::task::spawn_blocking(|| {}).await;
            done.set(true);
        })
        .expect("run should not fail");
    }
    assert!(done.get());
}

// Phase 3 Drop test: a BlockingTask dropped before completion removes its
// entry from `RuntimeState::blocking_wakers`. If it did not, the executor's
// termination check (`blocking_wakers.is_empty()`) would never pass and `run()`
// would poll forever. Runs against a hand-installed runtime handle so the
// blocking_wakers map can be inspected directly.
#[test]
fn dropped_spawn_blocking_leaves_blocking_map_empty() {
    let poll = mio::Poll::new().unwrap();
    let waker = Arc::new(mio::Waker::new(poll.registry(), WAKEN_TOKEN).unwrap());
    let worker_pool = WorkerPool::new(waker);
    let state = RuntimeState::new();
    let _context = context::install(context::ContextHandle {
        state: state.clone(),
        wheel: Rc::new(TimerHeap::new()),
        job_tx: worker_pool.job_tx(),
        completed_tx: worker_pool.completed_tx(),
    });

    let fut = crate::task::spawn_blocking(|| {
        thread::sleep(Duration::from_millis(50));
        7u32
    });
    let id = fut.id();
    let mut fut = Box::pin(fut);

    let waker = create_waker(state.borrow().queue.clone(), TaskId(999));
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());
    assert!(state.borrow().blocking_wakers.contains_key(&id));

    drop(fut);
    assert!(state.borrow().blocking_wakers.is_empty());
}

// Step 15 — shutdown: `run()` drops the Mar when it returns, which closes
// the job channel (the guard released the thread-local sender clone first) and
// joins the worker threads. If any sender leaked, `join()` would block
// forever — so `run()` returning promptly IS the assertion.
#[test]
fn runtime_drop_joins_workers_promptly_after_run() {
    let start = Instant::now();
    Mar::run(async {
        let _result = crate::task::spawn_blocking(|| 21 * 2).await;
    })
    .expect("run should not fail");
    assert!(start.elapsed() < Duration::from_millis(500));
}

// A panicking root future still joins workers: the unwind drops the guard,
// which uninstalls the thread-local context, then drops `Mar`, and the pool
// joins its workers. The panic propagates to the caller intact.
#[test]
fn panicking_root_future_still_joins_workers() {
    let start = Instant::now();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Mar::run(async move {
            let _job = crate::task::spawn_blocking(|| 21 * 2);
            panic!("root future exploded");
        })
        .expect("run should not fail");
    }));
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_millis(500));
}
