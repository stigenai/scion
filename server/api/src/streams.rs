//! Limits on server streams.
//!
//! The layers in [`crate::limit`] and `scion_obs::DeadlineLayer` stop at the response headers, so
//! they bound neither the time a server stream stays open nor how many streams one caller opens.
//! [`StreamLimits`] does both, inside the handler, so a stream ends between two messages:
//!
//! - [`StreamLimits::open`] takes one slot of the caller's budget (`--tenant-streams`) or fails with
//!   RESOURCE_EXHAUSTED / RATE_LIMITED. The slot is free again when the [`Ticket`] drops.
//! - [`StreamLimits::bound`] ends the stream with UNAVAILABLE after `--max-stream-lifetime`. The
//!   client SDK retries that status, so `scn watch` reconnects and resumes by version.
//!
//! A client that stops reading stalls the response body, so the timer in [`Bounded`] does not run.
//! `serve` wraps each connection in `age::Aged` as the backstop. It closes the connection after
//! `--max-stream-lifetime` plus a grace and counts the close as `connection_age`.
//!
//! Each hit counts in `scion_rejected_requests_total{reason}`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use scion_obs::reason;
use tokio::time::Sleep;
use tokio_stream::Stream;
use tonic::{Request, Status};

use crate::{limit, status};

/// Open streams per tenant key.
type Open = Arc<Mutex<HashMap<String, usize>>>;

/// How long a stream stays open, and how many streams one caller keeps open.
#[derive(Clone, Default)]
pub struct StreamLimits {
    lifetime: Option<Duration>,
    budget: Option<(usize, Open)>,
}

/// One open slot of a caller's stream budget. Dropping it frees the slot.
pub struct Ticket {
    slot: Option<(String, Open)>,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let Some((key, open)) = self.slot.take() else { return };
        let mut open = open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = open.get_mut(&key) {
            *n -= 1;
            if *n == 0 {
                open.remove(&key);
            }
        }
    }
}

impl StreamLimits {
    /// `lifetime` of `None` or zero leaves streams open as long as the client wants.
    /// `per_tenant` of `None` sets no budget.
    pub fn new(lifetime: Option<Duration>, per_tenant: Option<usize>) -> Self {
        Self {
            lifetime: lifetime.filter(|d| !d.is_zero()),
            budget: per_tenant.map(|n| (n.max(1), Arc::default())),
        }
    }

    /// Takes a slot of the budget of `caller`. Call it after the handler authorizes the request, so
    /// a refused call takes no slot.
    pub fn open(&self, caller: &str) -> Result<Ticket, Status> {
        let Some((max, open)) = &self.budget else {
            return Ok(Ticket { slot: None });
        };
        let key = caller.to_owned();
        let mut map = open.lock().unwrap_or_else(|e| e.into_inner());
        let n = map.entry(key.clone()).or_insert(0);
        if *n >= *max {
            scion_obs::metrics().reject(reason::TENANT_STREAMS);
            return Err(status::rate_limited(
                &format!("too many open streams for {key}"),
                Duration::from_secs(1),
            ));
        }
        *n += 1;
        Ok(Ticket {
            slot: Some((key, open.clone())),
        })
    }

    /// Ends `stream` with UNAVAILABLE when the lifetime is over. The ticket lives as long as the
    /// stream does.
    pub fn bound<T, S>(&self, ticket: Ticket, stream: S) -> Bounded<S>
    where
        S: Stream<Item = Result<T, Status>> + Unpin,
    {
        // ponytail: every stream gets the same lifetime. Clients that start together reconnect
        // together. Add jitter if reconnect herds show in scion_rpc_requests_total.
        Bounded {
            stream,
            deadline: self.lifetime.map(|d| (Box::pin(tokio::time::sleep(d)), d)),
            expired: false,
            _ticket: ticket,
        }
    }
}

/// The budget key of the caller of `request`. It is the key [`crate::limit::TenantLimitLayer`] uses.
/// Take it before `into_inner`, which drops the extensions.
pub fn caller<T>(request: &Request<T>) -> String {
    limit::tenant_key(
        request.extensions().get::<scion_auth::Principal>(),
        request.remote_addr().map(|a| a.ip()),
    )
}

/// A stream that ends with UNAVAILABLE at its deadline. See [`StreamLimits::bound`].
pub struct Bounded<S> {
    stream: S,
    deadline: Option<(Pin<Box<Sleep>>, Duration)>,
    expired: bool,
    _ticket: Ticket,
}

impl<T, S> Stream for Bounded<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.expired {
            return Poll::Ready(None);
        }
        // The deadline goes first. A stream of ready items (a finite answer) also ends at its
        // lifetime, and the sleep always has our waker. The UNAVAILABLE status makes the client retry.
        if let Some((sleep, lifetime)) = &mut this.deadline
            && sleep.as_mut().poll(cx).is_ready()
        {
            let lifetime = *lifetime;
            this.expired = true;
            scion_obs::metrics().reject(reason::STREAM_LIFETIME);
            // Not a clean end: that would silently truncate a stream with a finite answer.
            // No ErrorInfo reason, so the SDK retries on the code alone.
            return Poll::Ready(Some(Err(Status::unavailable(format!(
                "stream lifetime of {}s reached; reconnect",
                lifetime.as_secs()
            )))));
        }
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio_stream::StreamExt;
    use tonic::Code;

    fn hits(reason: &str) -> u64 {
        scion_obs::metrics()
            .encode()
            .lines()
            .find_map(|l| l.strip_prefix(&format!("scion_rejected_requests_total{{reason=\"{reason}\"}} ")))
            .map_or(0, |v| v.trim().parse().expect("counter"))
    }

    fn call_from(subject: &str) -> Request<()> {
        let mut r = Request::new(());
        r.extensions_mut()
            .insert(scion_auth::Principal::user(subject.to_owned(), vec![]));
        r
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_stream_ends_with_unavailable_at_its_lifetime() {
        let limits = StreamLimits::new(Some(Duration::from_secs(10)), None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<u32, Status>>(4);
        let mut s = limits.bound(
            limits.open(&caller(&call_from("a"))).expect("open"),
            tokio_stream::wrappers::ReceiverStream::new(rx),
        );
        let before = hits("stream_lifetime");
        for i in 1..=3 {
            tx.send(Ok(i)).await.expect("send");
            tokio::time::advance(Duration::from_secs(3)).await;
            assert_eq!(s.next().await.expect("item").expect("ok"), i);
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        let end = s.next().await.expect("an error, not a clean end").expect_err("ends");
        assert_eq!(end.code(), Code::Unavailable);
        assert_eq!(end.message(), "stream lifetime of 10s reached; reconnect");
        assert!(s.next().await.is_none(), "one status only");
        assert!(hits("stream_lifetime") > before, "other tests share the counter");
    }

    #[tokio::test(start_paused = true)]
    async fn zero_lifetime_never_ends_the_stream() {
        let limits = StreamLimits::new(Some(Duration::ZERO), None);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<u32, Status>>(1);
        let mut s = limits.bound(
            limits.open(&caller(&call_from("a"))).expect("open"),
            tokio_stream::wrappers::ReceiverStream::new(rx),
        );
        tokio::time::advance(Duration::from_secs(10 * 3600)).await;
        tx.send(Ok(1)).await.expect("send");
        assert_eq!(s.next().await.expect("item").expect("ok"), 1);
        drop(tx);
        assert!(s.next().await.is_none(), "a clean end stays clean");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_with_ready_items_ends_at_its_lifetime() {
        let limits = StreamLimits::new(Some(Duration::from_secs(10)), None);
        let mut s = limits.bound(
            limits.open(&caller(&call_from("a"))).expect("open"),
            tokio_stream::iter(vec![Ok(1u32), Ok(2), Ok(3)]),
        );
        assert_eq!(s.next().await.expect("item").expect("ok"), 1);
        tokio::time::advance(Duration::from_secs(11)).await;
        let end = s.next().await.expect("a status").expect_err("ends");
        assert_eq!(end.code(), Code::Unavailable);
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn a_finite_stream_ends_cleanly_before_its_lifetime() {
        let limits = StreamLimits::new(Some(Duration::from_secs(3600)), None);
        let s = limits.bound(
            limits.open(&caller(&call_from("a"))).expect("open"),
            tokio_stream::iter(vec![Ok(1u32), Ok(2)]),
        );
        let got: Vec<_> = s.collect().await;
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(Result::is_ok));
    }

    #[test]
    fn the_budget_is_per_caller_and_frees_on_drop() {
        let limits = StreamLimits::new(None, Some(2));
        let before = hits("tenant_streams");
        let a1 = limits.open(&caller(&call_from("alice"))).expect("1");
        let _a2 = limits.open(&caller(&call_from("alice"))).expect("2");
        let refused = limits.open(&caller(&call_from("alice"))).err().expect("3rd refused");
        assert_eq!(refused.code(), Code::ResourceExhausted);
        assert!(hits("tenant_streams") > before, "other tests share the counter");
        let _b = limits.open(&caller(&call_from("bob"))).expect("bob has his own budget");
        drop(a1);
        let _a3 = limits
            .open(&caller(&call_from("alice")))
            .expect("a dropped ticket frees a slot");
    }

    #[test]
    fn anonymous_callers_count_by_address() {
        let limits = StreamLimits::new(None, Some(1));
        let from = |addr: &str| {
            let mut r = Request::new(());
            r.extensions_mut().insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(addr.parse::<SocketAddr>().expect("addr")),
            });
            r
        };
        let _a = limits.open(&caller(&from("10.0.0.1:1000"))).expect("first");
        assert!(
            limits.open(&caller(&from("10.0.0.1:2000"))).is_err(),
            "same address, other port"
        );
        assert!(limits.open(&caller(&from("10.0.0.2:1000"))).is_ok());
    }

    #[test]
    fn no_budget_refuses_nothing() {
        let limits = StreamLimits::new(None, None);
        let _tickets: Vec<_> = (0..100)
            .map(|_| limits.open(&caller(&call_from("a"))).expect("open"))
            .collect();
    }
}
