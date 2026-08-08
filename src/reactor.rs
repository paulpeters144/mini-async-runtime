use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Duration;

pub(crate) type ReactorHandle = Rc<RefCell<Reactor>>;

pub struct Reactor {
    poll: mio::Poll,
}

impl Reactor {
    pub fn new() -> Self {
        let poll = mio::Poll::new().unwrap();
        Reactor { poll }
    }

    pub fn registry(&self) -> &mio::Registry {
        self.poll.registry()
    }

    pub fn poll(&mut self, events: &mut mio::Events, timeout: Option<Duration>) -> io::Result<()> {
        self.poll.poll(events, timeout)
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}
