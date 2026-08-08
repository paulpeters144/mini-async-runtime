---
title: mar runtime — the runtime primitives
---

```mermaid
classDiagram
    direction TB

    class Mar {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerHeap~
        reactor: Rc~RefCell~Reactor~
        pool: WorkerPool
        events: mio::Events
    }

    class RuntimeState {
        queue: Rc~RefCell~VecDeque~TaskId~~~
        tasks: HashMap~TaskId, Task~
        next_id: TaskId
        blocking_wakers: HashMap~BlockingId, Waker~
        next_blocking_id: BlockingId
    }

    class ContextHandle {
        state: Rc~RefCell~RuntimeState~~
        wheel: Rc~TimerHeap~
        job_tx: Sender~Job~
        completed_tx: Sender~BlockingId~
    }

    class Task {
        id: TaskId
        waker: Waker
        future: Pin~Box~dyn Future~Output=()~~~
    }

    class Waker {
        queue: Weak~RefCell~VecDeque~TaskId~~~
        id: TaskId
    }

    class TimerHeap {
        heap: RefCell~BinaryHeap~Reverse~TimerEntry~~~
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
    Mar --> TimerHeap
    Mar --> Reactor
    Mar --> WorkerPool
    ContextHandle --> RuntimeState
    ContextHandle --> TimerHeap
    RuntimeState --> Task : tasks
    RuntimeState --> Waker : blocking_wakers
    Task --> Waker : waker
    Waker ..> RuntimeState : wake() pushes to shared ready queue
    TimerHeap --> Waker : entry wakers
    WorkerPool --> Mar : wake() on job completion
```
