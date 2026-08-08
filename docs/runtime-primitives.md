---
title: mar runtime — the runtime primitives
---

```mermaid
classDiagram
    direction TB

    class Mar {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerRegistry~
        reactor: Rc~RefCell~Reactor~
        pool: WorkerPool
        events: mio::Events
    }

    class RuntimeState {
        queue: Arc~Mutex~Vec~TaskId~~
        tasks: HashMap~TaskId, Task~
        next_id: TaskId
        blocking_wakers: HashMap~BlockingId, Waker~
        next_blocking_id: BlockingId
    }

    class ContextHandle {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerRegistry~
        job_tx: Sender~Job~
        completed_tx: Sender~BlockingId~
    }

    class Task {
        id: TaskId
        waker: Waker
        future: Pin~Box~dyn Future~Output=()~~~
    }

    class TaskWaker {
        queue: Arc~Mutex~Vec~TaskId~~
        id: TaskId
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

    class Reactor {
        poll: mio::Poll
    }

    Mar --> RuntimeState
    Mar --> TimerRegistry
    Mar --> Reactor
    Mar --> WorkerPool
    ContextHandle --> RuntimeState
    ContextHandle --> TimerRegistry
    RuntimeState --> Task : tasks
    RuntimeState --> TaskWaker : blocking_wakers
    Task --> TaskWaker : waker
    TaskWaker ..> RuntimeState : wake() pushes to shared ready queue
    TimerRegistry --> TaskWaker : entry wakers
    WorkerPool --> Mar : wake() on job completion
```
