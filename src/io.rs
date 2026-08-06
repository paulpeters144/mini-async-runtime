use crate::reactor;
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

// A future that reads once from a source, returning the bytes.
//
// On its first poll it asks the shared reactor (via the thread-local `with`
// accessor) for a fresh token and, if the read would block, registers the
// source with the poller and parks the task's waker under that token. A later
// poll — after the reactor's `dispatch` wakes us because a readiness event
// fired — finally succeeds at reading, deregisters the source, and returns the
// bytes read. If the future is dropped before it completes, `Drop` deregisters
// so the source does not leave a stale entry blocking the termination check
// (the same cancellation discipline as `Sleep`).
pub struct Readable<T: mio::event::Source> {
    src: Option<T>,
    token: mio::Token,
    registered: bool,
    done: bool,
}

/// Build a future that reads bytes from the given source.
///
/// Mirrors `timer_wheel::sleep`: a free function that reaches the shared
/// reactor through the thread-local handle installed by `run()`. Calling it
/// outside `run()` panics with a clear message.
pub fn read<T: mio::event::Source>(src: T) -> Readable<T> {
    let token = reactor::with(|reactor| reactor.allocate_token());
    Readable {
        src: Some(src),
        token,
        registered: false,
        done: false,
    }
}

impl<T: Read + mio::event::Source + Unpin> Future for Readable<T> {
    type Output = Vec<u8>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Vec::new());
        }

        let src = this.src.as_mut().expect("Readable polled after completion");
        let mut buf = [0u8; 64];
        match src.read(&mut buf) {
            Ok(n) => {
                reactor::with(|reactor| {
                    let _ = reactor.deregister_source(src, this.token);
                });
                this.done = true;
                Poll::Ready(buf[..n].to_vec())
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !this.registered {
                    reactor::with(|reactor| {
                        reactor
                            .register_source(src, this.token, mio::Interest::READABLE)
                            .unwrap();
                    });
                    this.registered = true;
                }
                reactor::with(|reactor| reactor.register(this.token, cx.waker().clone()));
                Poll::Pending
            }
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

impl<T: mio::event::Source> Drop for Readable<T> {
    fn drop(&mut self) {
        if let Some(src) = self.src.as_mut() && !self.done {
            reactor::with(|reactor| {
                let _ = reactor.deregister_source(src, self.token);
            });
        }
    }
}

// A future that writes a whole buffer to a source.
//
// Writes, like reads, can block (`WouldBlock`); when one does, the future
// registers the source for writable interest and parks until the OS reports
// the socket can accept more bytes. A single poll may only write part of the
// buffer, so the future tracks where it left off and resumes from there on the
// next poll. When the whole buffer is flushed it deregisters and completes.
pub struct Writable<T: mio::event::Source> {
    src: Option<T>,
    token: mio::Token,
    registered: bool,
    buf: Vec<u8>,
    offset: usize,
    done: bool,
}

/// Build a future that writes `buf` (its contents are copied) to the source.
pub fn write<T: mio::event::Source>(src: T, buf: Vec<u8>) -> Writable<T> {
    let token = reactor::with(|reactor| reactor.allocate_token());
    Writable {
        src: Some(src),
        token,
        registered: false,
        buf,
        offset: 0,
        done: false,
    }
}

impl<T: Write + mio::event::Source + Unpin> Future for Writable<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(());
        }

        let src = this.src.as_mut().expect("Writable polled after completion");
        match src.write(&this.buf[this.offset..]) {
            Ok(n) => {
                this.offset += n;
                if this.offset == this.buf.len() {
                    reactor::with(|reactor| {
                        let _ = reactor.deregister_source(src, this.token);
                    });
                    this.done = true;
                    Poll::Ready(())
                } else {
                    if !this.registered {
                        reactor::with(|reactor| {
                            reactor
                                .register_source(src, this.token, mio::Interest::WRITABLE)
                                .unwrap();
                        });
                        this.registered = true;
                    }
                    reactor::with(|reactor| reactor.register(this.token, cx.waker().clone()));
                    Poll::Pending
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !this.registered {
                    reactor::with(|reactor| {
                        reactor
                            .register_source(src, this.token, mio::Interest::WRITABLE)
                            .unwrap();
                    });
                    this.registered = true;
                }
                reactor::with(|reactor| reactor.register(this.token, cx.waker().clone()));
                Poll::Pending
            }
            Err(e) => panic!("write failed: {e}"),
        }
    }
}

impl<T: mio::event::Source> Drop for Writable<T> {
    fn drop(&mut self) {
        if let Some(src) = self.src.as_mut() && !self.done {
            reactor::with(|reactor| {
                let _ = reactor.deregister_source(src, self.token);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mar::Mar;
    use std::cell::{Cell, RefCell};
    use std::io::{Read, Write};
    use std::rc::Rc;

    // A write on the tx end makes the rx end readable; `read` returns exactly
    // the bytes that were written.
    #[test]
    fn readable_returns_bytes_from_pair() {
        let (mut tx, rx) = mio::net::UnixStream::pair().unwrap();
        tx.write_all(b"hello").unwrap();

        let result = Rc::new(RefCell::new(Vec::new()));
        let result_writer = result.clone();
        Mar::run(async move {
            let bytes = read(rx).await;
            *result_writer.borrow_mut() = bytes;
        })
        .expect("run should not fail");

        assert_eq!(result.borrow().as_slice(), b"hello");
    }

    // If a `Readable` completes before a `read` is ever awaited, the source is
    // deregistered and reclaimed, and `run()` can return with an empty reactor.
    // This proves an already-readable socket does not leak a registration.
    #[test]
    fn readable_from_an_unparked_source_completes() {
        let (mut tx, rx) = mio::net::UnixStream::pair().unwrap();
        tx.write_all(b"x").unwrap();

        let got = Rc::new(Cell::new(false));
        let got_writer = got.clone();
        Mar::run(async move {
            let bytes = read(rx).await;
            assert_eq!(bytes, b"x");
            got_writer.set(true);
        })
        .expect("run should not fail");

        assert!(got.get());
    }

    // `Writable` works in isolation: the writer flushes its buffer into the tx
    // end of a socket pair, and the bytes show up on the rx end (read with a
    // plain non-async `read`). Small writes complete on the first poll without
    // ever registering with the reactor — `Writable` returns `Ready` immediately.
    #[test]
    fn writable_flushes_bytes_to_pair() {
        let (tx, mut rx) = mio::net::UnixStream::pair().unwrap();

        Mar::run(async move {
            write(tx, b"world".to_vec()).await;
        })
        .expect("run should not fail");

        let mut buf = [0u8; 64];
        let n = rx.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"world");
    }

    // Two independent socket pairs active at once: data is written before
    // `run()`, then both reads complete sequentially in one root future. The
    // token allocator hands out distinct tokens so the two `Readable` futures
    // do not collide on the shared poller.
    #[test]
    fn readable_and_writable_coexist_on_separate_pairs() {
        let (mut tx1, rx1) = mio::net::UnixStream::pair().unwrap();
        let (mut tx2, rx2) = mio::net::UnixStream::pair().unwrap();
        tx1.write_all(b"alpha").unwrap();
        tx2.write_all(b"beta").unwrap();

        let got1 = Rc::new(RefCell::new(Vec::new()));
        let got2 = Rc::new(RefCell::new(Vec::new()));
        let got1w = got1.clone();
        let got2w = got2.clone();

        Mar::run(async move {
            *got1w.borrow_mut() = read(rx1).await;
            *got2w.borrow_mut() = read(rx2).await;
        })
        .expect("run should not fail");

        assert_eq!(got1.borrow().as_slice(), b"alpha");
        assert_eq!(got2.borrow().as_slice(), b"beta");
    }

    // A 1 MB payload overflows the default UnixStream send buffer (~208 KB),
    // guaranteeing a partial write on the first poll.  After the fix, the
    // future must store its waker so the reactor can wake it when buffer space
    // frees up.
    #[test]
    fn writable_partial_write_stores_waker() {
        use crate::reactor;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        let runtime = Mar::new();
        reactor::install(runtime.reactor.clone());

        let (tx, _rx) = mio::net::UnixStream::pair().unwrap();
        let payload = vec![0xAB_u8; 1024 * 1024];
        let mut fut = Box::pin(super::write(tx, payload));

        fn noop_clone(_: *const ()) -> RawWaker { RawWaker::new(std::ptr::null(), &VTABLE) }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);

        let result = fut.as_mut().poll(&mut cx);
        assert_eq!(result, Poll::Pending);
        assert!(!runtime.reactor.borrow().is_empty(), "waker was not stored");
    }
}