use crate::task::Task;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::task::Waker;

pub struct RuntimeState {
    pub queue: VecDeque<usize>,
    pub tasks: HashMap<usize, Task>,
    pub next_id: usize,
    pub blocking_wakers: HashMap<usize, Waker>,
    pub(crate) next_blocking_id: usize,
}

impl RuntimeState {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(RuntimeState {
            queue: VecDeque::new(),
            tasks: HashMap::new(),
            next_id: 0,
            blocking_wakers: HashMap::new(),
            next_blocking_id: 0,
        }))
    }
}
