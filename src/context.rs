use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;

use crate::reactor::ReactorHandle;
use crate::runtime_state::RuntimeState;
use crate::time::TimerHeap;
use crate::task::worker_pool::Job;

pub(crate) struct ContextHandle {
    pub state: Rc<RefCell<RuntimeState>>,
    pub reactor: ReactorHandle,
    pub wheel: Rc<TimerHeap>,
    pub job_tx: mpsc::Sender<Job>,
}

thread_local! {
    static CONTEXT: RefCell<Option<ContextHandle>> = const { RefCell::new(None) };
}

pub(crate) fn install(handle: ContextHandle) {
    CONTEXT.with(|c| *c.borrow_mut() = Some(handle));
}

pub(crate) fn uninstall() {
    CONTEXT.with(|c| *c.borrow_mut() = None);
}

pub(crate) fn with<F, R>(f: F) -> R
where
    F: FnOnce(&ContextHandle) -> R,
{
    CONTEXT.with(|c| {
        f(c.borrow()
            .as_ref()
            .expect("runtime context not installed"))
    })
}
