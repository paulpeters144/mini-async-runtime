use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::task::Waker;
use std::time::Duration;

pub(crate) type ReactorHandle = Rc<RefCell<Reactor>>;

/// Reserved token for cross-thread worker-completion wakes (Phase 3).
pub const WAKEN_TOKEN: mio::Token = mio::Token(0);

pub struct Reactor {
    poll: mio::Poll,
    registry: HashMap<mio::Token, Waker>,
    next_token: usize,
}

impl Reactor {
    pub fn new() -> Self {
        let poll = mio::Poll::new().unwrap();
        let registry = HashMap::new();
        Reactor {
            poll,
            registry,
            next_token: 1, // Token(0) is reserved for WAKEN
        }
    }

    /// Hand out a unique token for an I/O future to register its source under.
    ///
    /// I/O futures call this (via `with`) when they are constructed so that
    /// each registered source gets its own distinct token on the shared poller.
    pub fn allocate_token(&mut self) -> mio::Token {
        let token = mio::Token(self.next_token);
        self.next_token += 1;
        token
    }

    pub fn registry(&self) -> &mio::Registry {
        self.poll.registry()
    }

    pub fn park(&mut self, events: &mut mio::Events, timeout: Option<Duration>) -> io::Result<()> {
        self.poll.poll(events, timeout)
    }

    pub fn register(&mut self, token: mio::Token, waker: Waker) {
        self.registry.insert(token, waker);
    }

    pub fn deregister(&mut self, token: mio::Token) {
        self.registry.remove(&token);
    }

    /// Register a source with the poller so its readiness fires `token` events.
    ///
    /// The I/O wrapper futures call this (via `with`) instead of touching the
    /// raw `mio::Registry` themselves.
    pub fn register_source(
        &mut self,
        source: &mut impl mio::event::Source,
        token: mio::Token,
        interest: mio::Interest,
    ) -> io::Result<()> {
        source.register(self.poll.registry(), token, interest)
    }

    /// Remove a source from the poller and forget its waker entry.
    pub fn deregister_source(
        &mut self,
        source: &mut impl mio::event::Source,
        token: mio::Token,
    ) -> io::Result<()> {
        source.deregister(self.poll.registry())?;
        self.registry.remove(&token);
        Ok(())
    }

    /// Part of `run()`'s termination check.
    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn dispatch(handle: &ReactorHandle, events: &mio::Events) {
    let reactor = handle.borrow_mut();
    for event in events.iter() {
        let token = event.token();
        if token == WAKEN_TOKEN {
            continue;
        }
        match reactor.registry.get(&token) {
            Some(waker) => waker.wake_by_ref(),
            None => panic!("event token {token:?} has no registered waker"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_state::RuntimeState;
    use crate::waker::create_waker;
    use mio::event::Source;
    use mio::{Interest, Token};
    use std::io::Write;
    use std::time::Instant;

    // Tokens must be unique per registered source, or two I/O futures would
    // collide on the same readiness event and wake the wrong task. `allocate`
    // returns a fresh, never-reused token each call.
    #[test]
    fn allocate_token_gives_distinct_tokens() {
        let mut reactor = Reactor::new();
        let first = reactor.allocate_token();
        let second = reactor.allocate_token();
        assert_ne!(first, second);
    }

    // The registry is the heart of the reactor: a map from `mio::Token` (an OS
    // readiness token) back to the `Waker` of the task parked on that source.
    // `is_empty()` is what the executor's termination check consults — the
    // reactor is only "done" when no task is parked waiting on a source.
    #[test]
    fn registry_maps_token_to_waker_until_deregistered() {
        let mut reactor = Reactor::new();
        assert!(reactor.is_empty());

        let state = RuntimeState::new();
        let waker = create_waker(state.clone(), 1);
        reactor.register(Token(7), waker);

        assert!(!reactor.is_empty());

        reactor.deregister(Token(7));
        assert!(reactor.is_empty());
    }

    // The full dispatch cycle: a write on one end of a `UnixStream::pair()`
    // makes the OS mark the other end readable; the reactor's `park` returns
    // with that readiness event; `dispatch` looks up the token and calls the
    // parked task's waker, which pushes the task's id onto the ready queue.
    // One write, one wakeup.
    #[test]
    fn dispatch_wakes_the_task_registered_under_the_event_token() {
        let handle: ReactorHandle = Rc::new(RefCell::new(Reactor::new()));
        let (mut tx, mut rx) = mio::net::UnixStream::pair().unwrap();
        rx.register(handle.borrow().registry(), Token(1), Interest::READABLE)
            .unwrap();

        let state = RuntimeState::new();
        let waker = create_waker(state.clone(), 1);
        handle.borrow_mut().register(Token(1), waker);

        tx.write_all(b"x").unwrap();

        let mut events = mio::Events::with_capacity(4);
        handle
            .borrow_mut()
            .park(&mut events, Some(Duration::from_secs(5)))
            .unwrap();

        assert_eq!(events.iter().count(), 1);
        assert_eq!(events.iter().next().unwrap().token(), Token(1));

        dispatch(&handle, &events);

        assert_eq!(state.borrow().queue, [1]);
    }

    // If `poll` returns a token with no registered waker, that is a bug in our
    // wake discipline — a silent skip would hide a mis-attributed `Ready` set.
    // So dispatch panics, loudly, instead of dropping the event.
    #[test]
    #[should_panic(expected = "no registered waker")]
    fn dispatch_panics_on_an_unmatched_event_token() {
        let handle: ReactorHandle = Rc::new(RefCell::new(Reactor::new()));
        let (mut tx, mut rx) = mio::net::UnixStream::pair().unwrap();
        rx.register(handle.borrow().registry(), Token(1), Interest::READABLE)
            .unwrap();
        // note: no waker is ever registered for Token(1)

        tx.write_all(b"x").unwrap();

        let mut events = mio::Events::with_capacity(4);
        handle
            .borrow_mut()
            .park(&mut events, Some(Duration::from_secs(5)))
            .unwrap();

        dispatch(&handle, &events);
    }

    // `park` is event-driven, not a fixed sleep: it blocks the thread until
    // either a registered source becomes ready or the timeout elapses. Here a
    // source is already readable, so the 5-second timeout is never consumed —
    // park returns as soon as the readiness event is available.
    #[test]
    fn park_returns_early_when_a_source_is_ready() {
        let handle: ReactorHandle = Rc::new(RefCell::new(Reactor::new()));
        let (mut tx, mut rx) = mio::net::UnixStream::pair().unwrap();
        rx.register(handle.borrow().registry(), Token(1), Interest::READABLE)
            .unwrap();

        tx.write_all(b"x").unwrap();

        let mut events = mio::Events::with_capacity(4);
        let start = Instant::now();
        handle
            .borrow_mut()
            .park(&mut events, Some(Duration::from_secs(5)))
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(events.iter().count(), 1);
        assert!(elapsed < Duration::from_secs(1));
    }

    // Level-triggered readiness: dispatching an event does NOT remove the waker
    // from the registry, so a still-readable source keeps waking its task on
    // every park. Only an explicit `deregister` (the I/O wrapper's `Drop`
    // discipline, mirroring the timer wheel) empties the registry — and that is
    // exactly what lets the executor's termination check pass.
    #[test]
    fn events_keep_waking_until_the_task_deregisters() {
        let handle: ReactorHandle = Rc::new(RefCell::new(Reactor::new()));
        let (mut tx, mut rx) = mio::net::UnixStream::pair().unwrap();
        rx.register(handle.borrow().registry(), Token(1), Interest::READABLE)
            .unwrap();

        let state = RuntimeState::new();
        let waker = create_waker(state.clone(), 1);
        handle.borrow_mut().register(Token(1), waker);

        let mut events = mio::Events::with_capacity(4);

        tx.write_all(b"a").unwrap();
        handle
            .borrow_mut()
            .park(&mut events, Some(Duration::from_secs(5)))
            .unwrap();
        dispatch(&handle, &events);
        assert_eq!(state.borrow().queue, [1]);

        // Still registered, still readable → the second write wakes it again.
        state.borrow_mut().queue.clear();
        tx.write_all(b"b").unwrap();
        handle
            .borrow_mut()
            .park(&mut events, Some(Duration::from_secs(5)))
            .unwrap();
        dispatch(&handle, &events);
        assert_eq!(state.borrow().queue, [1]);

        // The task deregisters: the fd leaves the OS poller and the waker
        // leaves the registry, so the reactor is empty again.
        rx.deregister(handle.borrow().registry()).unwrap();
        handle.borrow_mut().deregister(Token(1));
        assert!(handle.borrow().is_empty());
    }
}
