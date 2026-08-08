use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;

use crate::runtime_state::{BlockingId, RuntimeState};
use crate::time::TimerHeap;
use crate::task::worker_pool::Job;

pub(crate) struct ContextHandle {
    pub state: Rc<RefCell<RuntimeState>>,
    pub wheel: Rc<TimerHeap>,
    pub job_tx: mpsc::Sender<Job>,
    pub completed_tx: mpsc::Sender<BlockingId>,
}

thread_local! {
    static CONTEXT: RefCell<Option<ContextHandle>> = const { RefCell::new(None) };
}

/// RAII guard returned by `install`: dropping it uninstalls the thread-local
/// context. Declare it after the runtime so it drops first, releasing the
/// thread-local job sender before the pool joins its workers — on the happy
/// path and on the panic path alike.
#[must_use]
pub(crate) struct ContextGuard;

impl Drop for ContextGuard {
    fn drop(&mut self) {
        uninstall();
    }
}

pub(crate) fn install(handle: ContextHandle) -> ContextGuard {
    CONTEXT.with(|c| *c.borrow_mut() = Some(handle));
    ContextGuard
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
