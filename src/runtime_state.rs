use crate::task::Task;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::Waker;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TaskId(pub usize);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockingId(pub usize);

pub struct RuntimeState {
    pub queue: Arc<Mutex<Vec<TaskId>>>,
    pub tasks: HashMap<TaskId, Task>,
    pub next_id: TaskId,
    pub blocking_wakers: HashMap<BlockingId, Waker>,
    pub(crate) next_blocking_id: BlockingId,
}

impl RuntimeState {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(RuntimeState {
            queue: Arc::new(Mutex::new(Vec::new())),
            tasks: HashMap::new(),
            next_id: TaskId(0),
            blocking_wakers: HashMap::new(),
            next_blocking_id: BlockingId(0),
        }))
    }
}
