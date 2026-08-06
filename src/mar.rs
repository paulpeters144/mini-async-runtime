use crate::context;
use crate::reactor::{self, Reactor, ReactorHandle, WAKEN_TOKEN};
use crate::runtime_state::RuntimeState;
use crate::task::Task;
use crate::time::{self, TimerHeap};
use crate::waker::create_waker;
use crate::task::worker_pool::WorkerPool;
use std::cell::RefCell;
use std::future::Future;
use std::io::{self};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

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
            let reactor_ref = reactor.borrow();
            let waker = Arc::new(mio::Waker::new(reactor_ref.registry(), WAKEN_TOKEN).unwrap());
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

    pub fn run<F>(future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + 'static,
    {
        let mut runtime = Self::new();

        context::install(context::ContextHandle {
            state: runtime.state.clone(),
            reactor: runtime.reactor.clone(),
            wheel: runtime.wheel.clone(),
            job_tx: runtime.pool.job_tx(),
        });

        // Panic safety: if a task poll panics (e.g. a blocking closure's
        // payload is resumed inside `poll`), unwinding must still release the
        // thread-local job sender. Otherwise `WorkerPool::drop` would block
        // forever joining a worker whose channel never closed.
        struct ContextGuard;
        impl Drop for ContextGuard {
            fn drop(&mut self) {
                context::uninstall();
            }
        }
        let _guard = ContextGuard;

        {
            let mut state = runtime.state.borrow_mut();
            let id = state.next_id;
            state.next_id += 1;
            state.tasks.insert(id, Task::new(id, future));
            state.queue.push_back(id);
        }

        loop {
            loop {
                let next = {
                    let mut state = runtime.state.borrow_mut();
                    state.queue.pop_front()
                };
                let id = match next {
                    Some(id) => id,
                    None => break,
                };

                let mut task = match runtime.state.borrow_mut().tasks.remove(&id) {
                    Some(task) => task,
                    None => continue,
                };

                let waker = create_waker(runtime.state.clone(), id);
                let mut cx = Context::from_waker(&waker);
                match task.poll(&mut cx) {
                    Poll::Pending => {
                        runtime.state.borrow_mut().tasks.insert(id, task);
                    }
                    Poll::Ready(()) => {}
                }
            }

            let done = {
                let state = runtime.state.borrow();
                let reactor = runtime.reactor.borrow();
                state.tasks.is_empty()
                    && runtime.wheel.is_empty()
                    && reactor.is_empty()
                    && state.blocking_wakers.is_empty()
            };
            if done {
                return Ok(());
            }

            let timeout = time::next_deadline(&runtime.wheel).map(|deadline| {
                let now = Instant::now();
                if deadline > now {
                    deadline - now
                } else {
                    Duration::ZERO
                }
            });
            runtime
                .reactor
                .borrow_mut()
                .park(&mut runtime.events, timeout)?;

            time::expire_due(&runtime.wheel);

            for event in runtime.events.iter() {
                if event.token() == WAKEN_TOKEN {
                    // Collect first, then wake: `wake_by_ref` pushes the task
                    // id onto the queue — a `borrow_mut` on `state` — so the
                    // immutable borrow used for iteration must be released
                    // before any waker fires, or the RefCell double-borrows
                    // and panics.
                    let wakers: Vec<Waker> = {
                        let state = runtime.state.borrow();
                        state.blocking_wakers.values().cloned().collect()
                    };
                    for waker in &wakers {
                        waker.wake_by_ref();
                    }
                }
            }

            reactor::dispatch(&runtime.reactor, &runtime.events);
        }
    }
}

impl Default for Mar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::io::Write;
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
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
    assert_eq!(state.next_id, 0);
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

// A task parked on a socket: the I/O analogue of `Sleep`, now provided by the
// public `io::read` future. On its first `WouldBlock` poll it reaches the
// shared reactor through the thread-local `reactor::with` accessor, registering
// the read end with the poller and storing its waker under a token allocated by
// the reactor; returning `Pending` makes the executor park the thread. A write
// on the other end of the socket pair — from another thread — fires the
// readiness event; `dispatch` calls the stored waker, which re-queues the task;
// the next poll finds the bytes, deregisters, and completes. The task is then
// dropped, the reactor empties, and `run()` can return. The whole reactor in
// one `run()` call.
#[test]
fn run_wakes_a_task_parked_on_io_readiness() {
    let (tx, rx) = mio::net::UnixStream::pair().unwrap();

    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let mut tx = tx;
        tx.write_all(b"x").unwrap();
    });

    Mar::run(async move {
        let _ = crate::io::read(rx).await;
    })
    .expect("run should not fail");

    writer.join().unwrap();
}

// Smoke test: spawn_blocking called and awaited inside run().
#[test]
fn spawn_blocking_smoke() {
    let done = Rc::new(Cell::new(false));
    {
        let done = done.clone();
        Mar::run(async move {
            let _ = crate::task::blocking::spawn_blocking(|| {}).await;
            done.set(true);
        })
        .expect("run should not fail");
    }
    assert!(done.get());
}

// Phase 3 Drop test: a BlockingTask dropped before completion removes its
// entry from `RuntimeState::blocking`. If it did not, the executor's
// termination check (`blocking.is_empty()`) would never pass and `run()`
// would park forever. Runs against a hand-installed runtime handle so the
// blocking map can be inspected directly.
#[test]
fn dropped_spawn_blocking_leaves_blocking_map_empty() {
    let poll = mio::Poll::new().unwrap();
    let waker = Arc::new(mio::Waker::new(poll.registry(), WAKEN_TOKEN).unwrap());
    let pool = WorkerPool::new(waker);
    let state = RuntimeState::new();
    context::install(context::ContextHandle {
        state: state.clone(),
        reactor: Rc::new(RefCell::new(Reactor::new())),
        wheel: Rc::new(TimerHeap::new()),
        job_tx: pool.job_tx(),
    });

    let fut = crate::task::blocking::spawn_blocking(|| {
        thread::sleep(Duration::from_millis(50));
        7u32
    });
    let id = fut.id();
    let mut fut = Box::pin(fut);

    let waker = create_waker(state.clone(), id);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());
    assert!(state.borrow().blocking_wakers.contains_key(&id));

    drop(fut);
    assert!(state.borrow().blocking_wakers.is_empty());

    context::uninstall();
}

// Step 15 — shutdown: `run()` drops the Mar when it returns, which closes
// the job channel (the guard released the thread-local sender clone first) and
// joins the worker threads. If any sender leaked, `join()` would block
// forever — so `run()` returning promptly IS the assertion.
#[test]
fn runtime_drop_joins_workers_promptly_after_run() {
    let start = Instant::now();
    Mar::run(async {
        let _ = crate::task::blocking::spawn_blocking(|| 21 * 2).await;
    })
    .expect("run should not fail");
    assert!(start.elapsed() < Duration::from_millis(500));
}
