---
title: mar runtime — the four components
---

```mermaid
classDiagram
    direction LR

    class Executor {
        +queue: Arc~Mutex~VecDeque~TaskId~~~
        +tasks: HashMap~usize, Task~
    }

    class Reactor {
        +poll: mio Poll
    }

    class TimerRegistry {
        +entries: Vec~TimerEntry~
        +next_id: usize
    }

    class WorkerPool {
        +job_tx: Sender~Job~
        +workers: Vec~JoinHandle~
    }

    Executor --> Reactor : parks + wakes
    Executor --> TimerRegistry : checks deadlines
    Executor --> WorkerPool : offloads blocking work
```
