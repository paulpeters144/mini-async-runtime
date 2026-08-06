use std::future::Future;
use std::io;
use std::pin::Pin;

pub type ReadFuture<'a> = Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;
pub type WriteAllFuture<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

pub trait Connection: Send + 'static {
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a>;

    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAllFuture<'a>;
}

impl Connection for Box<dyn Connection> {
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        (**self).read(buf)
    }

    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAllFuture<'a> {
        (**self).write_all(buf)
    }
}
