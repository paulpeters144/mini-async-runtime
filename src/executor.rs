use crate::blocking::{self, WorkerPool};
use crate::reactor::{self, Reactor, ReactorHandle, WAKEN_TOKEN};
use crate::runtime_state::RuntimeState;
use crate::task::Task;
use crate::timer_wheel::{self, TimerWheel};
use crate::waker::create_waker;
use std::cell::RefCell;
use std::future::Future;
use std::io::{self};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

pub struct Runtime {
    state: Rc<RefCell<RuntimeState>>,
    wheel: TimerWheel,
    pub(crate) reactor: ReactorHandle,
    pool: WorkerPool,
    events: mio::Events,
}

impl Runtime {
    pub fn new() -> Self {
        let state = RuntimeState::new();
        let wheel = Rc::new(RefCell::new(std::collections::BinaryHeap::new()));
        let reactor = Rc::new(RefCell::new(Reactor::new()));
        let pool = {
            let reactor_ref = reactor.borrow();
            let waker = Arc::new(mio::Waker::new(reactor_ref.registry(), WAKEN_TOKEN).unwrap());
            WorkerPool::new(waker)
        };
        let events = mio::Events::with_capacity(64);
        Runtime {
            state,
            wheel,
            reactor,
            pool,
            events,
        }
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let mut state = self.state.borrow_mut();
        let id = state.next_id;
        state.next_id += 1;
        state.tasks.insert(id, Task::new(id, future));
        state.queue.push_back(id);
    }

    pub fn run(&mut self) -> io::Result<()> {
        timer_wheel::install(self.wheel.clone());
        reactor::install(self.reactor.clone());
        blocking::install(self.state.clone(), self.pool.job_tx());

        // Panic safety: if a task poll panics (e.g. a blocking closure's
        // payload is resumed inside `poll`), unwinding must still release the
        // thread-local job sender. Otherwise `WorkerPool::drop` would block
        // forever joining a worker whose channel never closed.
        struct BlockingUninstall;
        impl Drop for BlockingUninstall {
            fn drop(&mut self) {
                blocking::uninstall();
            }
        }
        let _guard = BlockingUninstall;

        loop {
            loop {
                let next = {
                    let mut state = self.state.borrow_mut();
                    state.queue.pop_front()
                };
                let id = match next {
                    Some(id) => id,
                    None => break,
                };

                let mut task = match self.state.borrow_mut().tasks.remove(&id) {
                    Some(task) => task,
                    None => continue,
                };

                timer_wheel::set_current_id(id);
                let waker = create_waker(self.state.clone(), id);
                let mut cx = Context::from_waker(&waker);
                match task.poll(&mut cx) {
                    Poll::Pending => {
                        self.state.borrow_mut().tasks.insert(id, task);
                    }
                    Poll::Ready(()) => {}
                }
                timer_wheel::clear_current_id();
            }

            let done = {
                let state = self.state.borrow();
                let wheel = self.wheel.borrow();
                let reactor = self.reactor.borrow();
                state.tasks.is_empty()
                    && wheel.is_empty()
                    && reactor.is_empty()
                    && state.blocking.is_empty()
            };
            if done {
                return Ok(());
            }

            let timeout = timer_wheel::next_deadline(&self.wheel).map(|deadline| {
                let now = Instant::now();
                if deadline > now {
                    deadline - now
                } else {
                    Duration::ZERO
                }
            });
            self.reactor.borrow_mut().park(&mut self.events, timeout)?;

            for id in timer_wheel::expire_due(&self.wheel) {
                self.state.borrow_mut().queue.push_back(id);
            }

            for event in self.events.iter() {
                if event.token() == WAKEN_TOKEN {
                    // Collect first, then wake: `wake_by_ref` pushes the task
                    // id onto the queue — a `borrow_mut` on `state` — so the
                    // immutable borrow used for iteration must be released
                    // before any waker fires, or the RefCell double-borrows
                    // and panics.
                    let wakers: Vec<Waker> = {
                        let state = self.state.borrow();
                        state.blocking.values().cloned().collect()
                    };
                    for waker in &wakers {
                        waker.wake_by_ref();
                    }
                }
            }

            reactor::dispatch(&self.reactor, &self.events);
        }
    }
}

impl Default for Runtime {
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
// allocation: an empty `RuntimeState` with an empty queue and no task map. A
// fresh runtime has nothing registered yet — `spawn()` is the only way in.
#[test]
fn new_runtime_has_empty_queue_and_task_map() {
    let runtime = Runtime::new();
    let state = runtime.state.borrow();
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
    assert_eq!(state.next_id, 0);
}

// `spawn` is pure bookkeeping: it hands the future to the runtime and records
// it in two places. The `tasks` map stores the future itself (so the executor
// can find it later), and the `queue` gets the id (the executor's "to-do
// list"). The id comes from `next_id`, which increments with every spawn — so
// the first spawn is task 0, the second is task 1.
#[test]
fn spawn_assigns_id_and_registers_the_future() {
    let mut runtime = Runtime::new();
    runtime.spawn(async {});
    runtime.spawn(async {});

    let state = runtime.state.borrow();
    assert_eq!(state.next_id, 2);
    assert_eq!(state.queue, [0, 1]);
    assert_eq!(state.tasks[&0].id(), 0);
    assert_eq!(state.tasks[&1].id(), 1);
}

// The termination condition: `run()` exits when the task map is empty. With
// nothing spawned that is true immediately — no polling, no parking, no
// sleeping. A runtime with no work is done before it starts.
#[test]
fn run_returns_immediately_when_nothing_is_spawned() {
    let mut runtime = Runtime::new();
    runtime.run().expect("run should not fail");
    let state = runtime.state.borrow();
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
}

// The happy path: an `async {}` block has no awaits, so its first poll is also
// its last — it reports `Ready` and the executor drops it. Three spawned tasks
// are each polled once and disposed of, so after `run()` the shared counter
// equals the number of tasks, and both queue and map are empty: nothing is
// stranded (the invariant "termination is total").
#[test]
fn run_completes_spawned_tasks_and_leaves_empty_state() {
    let polls = Rc::new(Cell::new(0usize));
    let mut runtime = Runtime::new();
    for _ in 0..3 {
        let polls = polls.clone();
        runtime.spawn(async move {
            polls.set(polls.get() + 1);
        });
    }

    runtime.run().expect("run should not fail");

    assert_eq!(polls.get(), 3);
    let state = runtime.state.borrow();
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
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
    let mut runtime = Runtime::new();
    runtime.spawn(Probe {
        target: 3,
        polls: polls.clone(),
    });

    runtime.run().expect("run should not fail");

    assert_eq!(polls.get(), 3);
    let state = runtime.state.borrow();
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
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
    let mut runtime = Runtime::new();
    runtime.spawn(async move {
        let _ = crate::io::read(rx).await;
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let mut tx = tx;
        tx.write_all(b"x").unwrap();
    });

    runtime.run().expect("run should not fail");

    let state = runtime.state.borrow();
    assert!(state.queue.is_empty());
    assert!(state.tasks.is_empty());
    assert!(runtime.reactor.borrow().is_empty());
}

// The whole runtime in one `run()`: two tasks over a single socket pair. The
// writer task flushes `io::write(tx, …)` while the reader task receives
// `io::read(rx); …`; when the writer runs first the read might block and park
// on the reactor, and the readiness event from the write wakes it. Either way
// both tasks interleave on one thread, both complete, the reactor empties, and
// `run()` returns.
#[test]
fn two_tasks_exchange_bytes_over_a_socket_pipe() {
    let (tx, rx) = mio::net::UnixStream::pair().unwrap();
    let mut runtime = Runtime::new();

    runtime.spawn(async move {
        crate::io::write(tx, b"ping".to_vec()).await;
    });

    let got = Rc::new(RefCell::new(Vec::new()));
    let got_writer = got.clone();
    runtime.spawn(async move {
        let bytes = crate::io::read(rx).await;
        *got_writer.borrow_mut() = bytes;
    });

    runtime.run().expect("run should not fail");

    assert_eq!(got.borrow().as_slice(), b"ping");
    assert!(runtime.reactor.borrow().is_empty());
}

// Smoke test: spawn_blocking called and awaited inside run().
#[test]
fn spawn_blocking_smoke() {
    let mut runtime = Runtime::new();
    let done = Rc::new(Cell::new(false));
    {
        let done = done.clone();
        runtime.spawn(async move {
            let _ = crate::blocking::spawn_blocking(|| {}).await;
            done.set(true);
        });
    }
    runtime.run().expect("run should not fail");
    assert!(done.get());
}

// Phase 3 Drop test: a BlockingTask dropped before completion removes its
// entry from `RuntimeState::blocking`. If it did not, the executor's
// termination check (`blocking.is_empty()`) would never pass and `run()`
// would park forever.
#[test]
fn dropped_spawn_blocking_leaves_blocking_map_empty() {
    let mut runtime = Runtime::new();
    runtime.spawn(async {
        let fut = crate::blocking::spawn_blocking(|| {
            thread::sleep(Duration::from_millis(50));
            7u32
        });
        let state = fut.state();
        let id = fut.id();
        let mut fut = Box::pin(fut);

        // First poll registers the task's waker in the blocking map and pends.
        let waker = create_waker(state.clone(), id);
        let mut cx = Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(state.borrow().blocking.contains_key(&id));

        // Cancel: drop before the worker finishes. The entry must go.
        drop(fut);
        assert!(state.borrow().blocking.is_empty());
    });

    runtime.run().expect("run should not fail");
}

// Step 15 — shutdown: after `run()` returns, dropping the Runtime closes the
// job channel (run's guard released the thread-local sender clone) and joins
// the worker threads. If any sender leaked, `join()` would block forever —
// so this drop returning promptly IS the assertion.
#[test]
fn runtime_drop_joins_workers_promptly_after_run() {
    let mut runtime = Runtime::new();
    runtime.spawn(async {
        let _ = crate::blocking::spawn_blocking(|| 21 * 2).await;
    });
    runtime.run().expect("run should not fail");

    let start = Instant::now();
    drop(runtime);
    assert!(start.elapsed() < Duration::from_millis(500));
}
