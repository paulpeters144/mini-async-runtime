use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct JoinAll<F: Future, const N: usize> {
    futures: [Option<F>; N],
    outputs: [Option<F::Output>; N],
    remaining: usize,
}

impl<F: Future, const N: usize> JoinAll<F, N> {
    pub fn new(futures: [F; N]) -> Self {
        JoinAll {
            futures: futures.map(Some),
            outputs: std::array::from_fn(|_| None),
            remaining: N,
        }
    }
}

impl<F: Future, const N: usize> Future for JoinAll<F, N> {
    type Output = [F::Output; N];

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        for i in 0..N {
            if let Some(fut) = &mut this.futures[i] {
                match unsafe { Pin::new_unchecked(fut) }.poll(cx) {
                    Poll::Ready(val) => {
                        this.outputs[i] = Some(val);
                        this.futures[i] = None;
                        this.remaining -= 1;
                    }
                    Poll::Pending => {}
                }
            }
        }

        if this.remaining == 0 {
            Poll::Ready(std::array::from_fn(|i| this.outputs[i].take().unwrap()))
        } else {
            Poll::Pending
        }
    }
}

pub fn all<F: Future, const N: usize>(futures: [F; N]) -> JoinAll<F, N> {
    JoinAll::new(futures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    struct OnceReady<T>(Option<T>);

    impl<T> Future for OnceReady<T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
            let this = unsafe { self.get_unchecked_mut() };
            Poll::Ready(this.0.take().unwrap())
        }
    }

    struct AlwaysPending;

    impl Future for AlwaysPending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    struct YieldTwice {
        yielded: usize,
    }

    impl Future for YieldTwice {
        type Output = u32;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
            self.yielded += 1;
            if self.yielded < 2 {
                Poll::Pending
            } else {
                Poll::Ready(42)
            }
        }
    }

    #[test]
    fn all_single_returns_value() {
        let mut join = JoinAll::new([OnceReady(Some(42u32))]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Ready([42]));
    }

    #[test]
    fn all_zero_immediately_ready() {
        let mut join = JoinAll::<AlwaysPending, 0>::new([]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Ready([]));
    }

    #[test]
    fn all_pending_then_ready_resumes() {
        let mut join = JoinAll::new([YieldTwice { yielded: 0 }]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Pending);
        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Ready([42]));
    }

    #[test]
    fn all_three_complete_in_order() {
        let mut join = JoinAll::new([
            OnceReady(Some(10u32)),
            OnceReady(Some(20u32)),
            OnceReady(Some(30u32)),
        ]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Ready([10, 20, 30]));
    }

    #[test]
    fn all_unfinished_yields_pending() {
        let mut join = JoinAll::new([AlwaysPending, AlwaysPending]);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert_eq!(Pin::new(&mut join).poll(&mut cx), Poll::Pending);
    }

    #[test]
    fn all_with_spawn_blocking() {
        use crate::Mar;
        use crate::task::spawn_blocking;

        Mar::run(async move {
            let [a, b, c] = all([
                spawn_blocking(|| "first"),
                spawn_blocking(|| "second"),
                spawn_blocking(|| "third"),
            ])
            .await;
            assert_eq!(a, "first");
            assert_eq!(b, "second");
            assert_eq!(c, "third");
        })
        .expect("run");
    }

    #[test]
    fn all_with_sleep_concurrent() {
        use crate::Mar;
        use crate::time::sleep;
        use std::time::{Duration, Instant};

        let elapsed = Rc::new(Cell::new(Duration::ZERO));
        {
            let elapsed = elapsed.clone();
            Mar::run(async move {
                let start = Instant::now();
                let [(), ()] = all([
                    sleep(Duration::from_millis(50)),
                    sleep(Duration::from_millis(50)),
                ])
                .await;
                elapsed.set(start.elapsed());
            })
            .expect("run");
        }

        let t = elapsed.get();
        assert!(t >= Duration::from_millis(50));
        assert!(t < Duration::from_millis(90));
    }
}
