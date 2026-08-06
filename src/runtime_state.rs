use crate::task::Task;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::task::Waker;

#[derive(Default)]
pub struct RuntimeState {
    pub queue: VecDeque<usize>,
    pub tasks: HashMap<usize, Task>,
    pub next_id: usize,
    pub blocking: HashMap<usize, Waker>,
}

impl RuntimeState {
    pub fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(RuntimeState {
            queue: VecDeque::new(),
            tasks: HashMap::new(),
            next_id: 0,
            blocking: HashMap::new(),
        }))
    }
}
