//! A hard limit on the age of a connection.
//!
//! A client that keeps its HTTP/2 window at zero holds a stream for ever, because hyper does not
//! poll the body while a chunk waits for send capacity. [`Aged`] is the backstop: after the hard
//! limit every read and write fails, so the connection drops with its streams. It sends no
//! GOAWAY. Tonic 0.14.5 `max_connection_age` cannot do it: with no grace it panics after the
//! graceful shutdown (transport/server/mod.rs:891), and with a grace it only closes.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use scion_obs::reason;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Sleep, sleep};
use tonic::transport::server::Connected;

/// An IO stream that fails after a fixed age. The failure counts once in
/// `scion_rejected_requests_total{reason="connection_age"}`.
pub struct Aged<T> {
    io: T,
    timer: Option<Pin<Box<Sleep>>>,
    counted: bool,
}

impl<T> Aged<T> {
    /// `None` sets no limit.
    pub fn new(io: T, age: Option<Duration>) -> Self {
        Self {
            io,
            timer: age.map(|a| Box::pin(sleep(a))),
            counted: false,
        }
    }

    fn check(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(timer) = self.timer.as_mut() else {
            return Poll::Pending;
        };
        ready!(timer.as_mut().poll(cx));
        if !std::mem::replace(&mut self.counted, true) {
            scion_obs::metrics().reject(reason::CONNECTION_AGE);
        }
        Poll::Ready(())
    }
}

fn expired() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "connection reached its maximum age")
}

impl<T: AsyncRead + Unpin> AsyncRead for Aged<T> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.check(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Aged<T> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.check(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.check(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl<T: Connected> Connected for Aged<T> {
    type ConnectInfo = T::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn io_works_until_the_age_and_fails_after_it() {
        let (a, mut b) = tokio::io::duplex(64);
        let mut aged = Aged::new(a, Some(Duration::from_secs(10)));
        b.write_all(b"hi").await.expect("write");
        let mut buf = [0u8; 2];
        aged.read_exact(&mut buf).await.expect("read before the age");
        tokio::time::sleep(Duration::from_secs(11)).await;
        let read = aged.read(&mut buf).await.expect_err("read after the age");
        assert_eq!(read.kind(), io::ErrorKind::TimedOut);
        let write = aged.write_all(b"x").await.expect_err("write after the age");
        assert_eq!(write.kind(), io::ErrorKind::TimedOut);
    }
}
