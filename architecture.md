---
title: mar architecture
---

## Top-Level Components

The executor and the subsystems it composes to form a working runtime.

```mermaid
classDiagram
    direction LR

    class Mar {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerRegistry~
        reactor: ReactorHandle
        pool: WorkerPool
        events: mio::Events
    }

    class RuntimeState {
        queue: ReadyQueue
        tasks: HashMap~TaskId, Task~
        next_id: TaskId
        blocking_wakers: HashMap~BlockingId, Waker~
        next_blocking_id: BlockingId
    }

    class Reactor {
        poll: mio::Poll
    }

    class TimerRegistry {
        entries: RefCell~Vec~TimerEntry~~
        next_id: Cell~usize~
    }

    class WorkerPool {
        job_tx: Option~Sender~Job~~
        workers: Vec~JoinHandle~
        completed_tx: Sender~BlockingId~
        completed_rx: Receiver~BlockingId~
    }

    Mar --> RuntimeState
    Mar --> Reactor
    Mar --> TimerRegistry
    Mar --> WorkerPool
    Reactor --> Mar : I/O events wake executor
    TimerRegistry --> Mar : expired timers push to ready queue
    WorkerPool --> Mar : wake() on job completion
```

- [Mar](docs/mar.md) — the executor loop: poll I/O, drain timers, run tasks
- [Reactor](docs/reactor.md) — mio-based I/O event polling
- [TimerRegistry](docs/timer-registry.md) — deadline tracking and timer expiry
- [WorkerPool](docs/worker-pool.md) — thread pool for `spawn_blocking`

## Runtime Primitives

The foundational scheduling abstractions. Every future ultimately interacts with these four.

```mermaid
classDiagram
    direction LR

    class ContextHandle {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerRegistry~
        job_tx: mpsc::Sender~Job~
        completed_tx: mpsc::Sender~BlockingId~
    }

    class RuntimeState {
        queue: ReadyQueue
        tasks: HashMap~TaskId, Task~
        next_id: TaskId
        blocking_wakers: HashMap~BlockingId, Waker~
        next_blocking_id: BlockingId
    }

    class Task {
        id: TaskId
        waker: Waker
        future: BoxedFuture
    }

    class TaskWaker {
        queue: ReadyQueue
        id: TaskId
    }

    ContextHandle --> RuntimeState
    RuntimeState --> Task : tasks
    RuntimeState --> TaskWaker : blocking_wakers
    Task --> TaskWaker : waker
    TaskWaker ..> RuntimeState : wake() pushes to ready queue
```

- [ContextHandle](docs/context-handle.md) — thread-local handle to reach runtime services
- [RuntimeState](docs/runtime-state.md) — scheduling state: ready queue, task table, blocking wakers
- [Task](docs/task.md) — a pinned, boxed future + waker, the unit of work
- [TaskWaker](docs/task-waker.md) — the `Wake` implementation that re-queues a task
