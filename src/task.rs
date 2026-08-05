use std::future::Future;
use std::pin::Pin;

pub struct Task {
    id: usize,
    future: Pin<Box<dyn Future<Output = ()>>>,
}
