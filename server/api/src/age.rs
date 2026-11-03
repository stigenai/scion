//! A hard limit on the age of a connection.
//!
//! A client that keeps its HTTP/2 window at zero holds a stream for ever, because hyper does not
//! poll the body while a chunk waits for send capacity. [`Aged`] is the backstop: after the hard
//! limit every read, write, flush and shutdown fails, so the connection drops with its streams. It sends no
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

/// The age at which a connection closes: the stream lifetime plus a grace.
/// The grace is the longer of two values: the request timeout, and 30s (but not more than the
/// lifetime). If a request timeout is set, a unary call that starts before the lifetime ends
/// finishes in the grace. With no request timeout (`None`), the grace is 30s and a longer
/// call is cut off.
pub fn hard_age(lifetime: Duration, request_timeout: Option<Duration>) -> Duration {
    lifetime
        + lifetime
            .min(Duration::from_secs(30))
            .max(request_timeout.unwrap_or_default())
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
    // Writes check the age too. hyper and h2 can stop reading (a full write buffer, a pending
    // PONG, a closing state), so a check on reads alone would not end such a connection.
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
        if self.check(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
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
#[allow(clippy::await_holding_lock)] // each test runs on its own current-thread runtime
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// The tests that expire a connection share one global counter, so they run one at a time.
    static EXPIRES: Mutex<()> = Mutex::new(());
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn io_works_until_the_age_and_fails_after_it() {
        let _one = EXPIRES.lock().unwrap_or_else(|e| e.into_inner());
        let (a, mut b) = tokio::io::duplex(64);
        let mut aged = Aged::new(a, Some(Duration::from_secs(10)));
        b.write_all(b"hi").await.expect("write");
        let mut buf = [0u8; 2];
        aged.read_exact(&mut buf).await.expect("read before the age");
        tokio::time::sleep(Duration::from_secs(11)).await;
        let read = aged.read(&mut buf).await.expect_err("read after the age");
        assert_eq!(read.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn the_grace_is_the_longer_of_30s_and_the_request_timeout() {
        let s = Duration::from_secs;
        assert_eq!(hard_age(s(3600), Some(s(60))), s(3660));
        assert_eq!(hard_age(s(3600), Some(s(10))), s(3630));
        assert_eq!(hard_age(s(3600), None), s(3630));
        assert_eq!(hard_age(s(5), Some(s(1))), s(10));
    }

    #[tokio::test(start_paused = true)]
    async fn pending_write_and_flush_fail_after_the_age() {
        let _one = EXPIRES.lock().unwrap_or_else(|e| e.into_inner());
        let (a, _b) = tokio::io::duplex(1);
        let mut aged = Aged::new(a, Some(Duration::from_secs(10)));
        aged.write_all(b"x").await.expect("fills the pipe");
        let write = aged.write_all(b"y").await.expect_err("write after the age");
        assert_eq!(write.kind(), io::ErrorKind::TimedOut);
        let flush = aged.flush().await.expect_err("flush after the age");
        assert_eq!(flush.kind(), io::ErrorKind::TimedOut);
        let shutdown = aged.shutdown().await.expect_err("shutdown after the age");
        assert_eq!(shutdown.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_counts_the_connection_once() {
        let _one = EXPIRES.lock().unwrap_or_else(|e| e.into_inner());
        let count = || {
            scion_obs::metrics()
                .rejected
                .get_or_create(&scion_obs::metrics::ReasonLabels {
                    reason: reason::CONNECTION_AGE.to_owned(),
                })
                .get()
        };
        let before = count();
        let (a, _b) = tokio::io::duplex(64);
        let mut aged = Aged::new(a, Some(Duration::from_secs(1)));
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut buf = [0u8; 1];
        aged.read(&mut buf).await.expect_err("read");
        aged.write_all(b"x").await.expect_err("write");
        aged.flush().await.expect_err("flush");
        assert_eq!(count(), before + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn no_age_never_expires() {
        let (a, mut b) = tokio::io::duplex(64);
        let mut aged = Aged::new(a, None);
        tokio::time::sleep(Duration::from_secs(100_000)).await;
        aged.write_all(b"hi").await.expect("write");
        let mut buf = [0u8; 2];
        b.read_exact(&mut buf).await.expect("peer reads");
    }
}
