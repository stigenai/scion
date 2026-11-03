//! Limits on server streams: the lifetime of a stream and the number of streams one caller keeps open.
//!
//! The limiter is [`scion_obs::streams`], which the cache node shares. This module adds what
//! needs the API crate: the RESOURCE_EXHAUSTED / RATE_LIMITED refusal and the budget key of a
//! request ([`caller`]). See `scion_obs::streams` for the lifetime and the connection age backstop.

use std::time::Duration;

use tokio_stream::Stream;
use tonic::{Request, Status};

pub use scion_obs::streams::{Bounded, Ticket};

use crate::{limit, status};

/// How long a stream stays open, and how many streams one caller keeps open.
#[derive(Clone, Default)]
pub struct StreamLimits(scion_obs::streams::StreamLimits);

impl StreamLimits {
    /// `lifetime` of `None` or zero leaves streams open as long as the client wants.
    /// `per_tenant` of `None` sets no budget.
    pub fn new(lifetime: Option<Duration>, per_tenant: Option<usize>) -> Self {
        Self(scion_obs::streams::StreamLimits::new(lifetime, per_tenant))
    }

    /// Takes a slot of the budget of `caller`, or fails with RESOURCE_EXHAUSTED / RATE_LIMITED.
    /// Call it after the handler authorizes the request, so a refused call takes no slot.
    pub fn open(&self, caller: &str) -> Result<Ticket, Status> {
        self.0.open_with(caller, |key| {
            status::rate_limited(&format!("too many open streams for {key}"), Duration::from_secs(1))
        })
    }

    /// Ends `stream` with UNAVAILABLE when the lifetime is over. The ticket lives as long as the
    /// stream does.
    pub fn bound<T, S>(&self, ticket: Ticket, stream: S) -> Bounded<S>
    where
        S: Stream<Item = Result<T, Status>> + Unpin,
    {
        self.0.bound(ticket, stream)
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
