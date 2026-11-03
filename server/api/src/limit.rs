//! 过载保护（服务端设计文档 §5.3）：
//!
//! - [`ConcurrencyLimitLayer`]：全局在途请求上限，超过时立即拒绝（load shed），不排队，
//!   客户端收到可重试的 RESOURCE_EXHAUSTED / RATE_LIMITED；健康检查不受限制；
//! - [`RateLimiter`]：按仓库的写入令牌桶，在写入类接口（上传、创建提交、合入）中检查，
//!   避免一个仓库耗尽整个服务的写入能力；
//! - [`TenantLimitLayer`]：按调用者（租户）的请求速率与在途请求上限，避免一个调用者占满整个服务。
//!   认证过的调用者按主体计，匿名调用者按来源 IP 计。
//!
//! 被拒绝的请求计入 `scion_rejected_requests_total{reason}`。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use scion_obs::reason;
use tokio::sync::Semaphore;
use tonic::Status;
use tower::{Layer, Service};

use crate::status;

/// 同时跟踪的 key 数上限：匿名调用者按 IP 计，不能无限增长。
const MAX_KEYS: u64 = 1_000_000;
/// 空闲这么久的 key 被淘汰：令牌桶早已补满，淘汰后重新开始不改变限流结果。
const KEY_IDLE: Duration = Duration::from_secs(600);

type Bucket = Arc<Mutex<(f64, Instant)>>;

/// 按 key（仓库或租户）的令牌桶：每秒补充 `rate` 个，最多积攒 `burst` 个。
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: moka::sync::Cache<String, Bucket>,
}

impl RateLimiter {
    pub fn new(per_second: u32, burst: u32) -> Self {
        Self {
            rate: f64::from(per_second.max(1)),
            burst: f64::from(burst.max(1)),
            buckets: moka::sync::Cache::builder()
                .max_capacity(MAX_KEYS)
                .time_to_idle(KEY_IDLE)
                .build(),
        }
    }

    /// 取一个令牌；没有时返回需要等待的时间。
    pub fn check(&self, key: &str) -> Result<(), Duration> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: &str, now: Instant) -> Result<(), Duration> {
        let bucket = self
            .buckets
            .get_with_by_ref(key, || Arc::new(Mutex::new((self.burst, now))));
        let Ok(mut b) = bucket.lock() else {
            return Ok(());
        };
        let (tokens, last) = &mut *b;
        let elapsed = now.saturating_duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate).min(self.burst);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - *tokens) / self.rate))
        }
    }

    /// 检查并转换成 gRPC 状态。
    pub fn admit(limiter: Option<&RateLimiter>, repo: &str) -> Result<(), Status> {
        match limiter.map(|l| l.check(repo)) {
            Some(Err(wait)) => {
                scion_obs::metrics().reject(reason::REPO_RATE);
                Err(status::rate_limited(
                    &format!("write rate limit for repository {repo:?} exceeded"),
                    wait,
                ))
            }
            _ => Ok(()),
        }
    }
}

/// 全局在途请求上限。
#[derive(Clone)]
pub struct ConcurrencyLimitLayer {
    permits: Arc<Semaphore>,
}

impl ConcurrencyLimitLayer {
    pub fn new(max: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max.max(1))),
        }
    }
}

impl<S> Layer<S> for ConcurrencyLimitLayer {
    type Service = ConcurrencyLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyLimit {
            inner,
            permits: self.permits.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ConcurrencyLimit<S> {
    inner: S,
    permits: Arc<Semaphore>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for ConcurrencyLimit<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // 按 tower 的约定：调用已就绪的那个实例，留一个克隆给下一次
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        if req.uri().path().starts_with("/grpc.health.v1.") {
            return Box::pin(inner.call(req));
        }
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let fut = inner.call(req);
                Box::pin(async move {
                    let resp = fut.await;
                    drop(permit);
                    resp
                })
            }
            Err(_) => {
                scion_obs::metrics().reject(reason::OVERLOAD);
                let s = status::rate_limited(
                    "server overloaded: too many requests in flight",
                    Duration::from_millis(100),
                );
                Box::pin(std::future::ready(Ok(s.into_http())))
            }
        }
    }
}

/// 按调用者（租户）的限制。放在 [`crate::auth::AuthLayer`] 之内：要用它认证出的身份。
#[derive(Clone, Default)]
pub struct TenantLimitLayer {
    rate: Option<Arc<RateLimiter>>,
    /// 每个租户的在途请求上限与各租户的名额。
    concurrency: Option<(usize, moka::sync::Cache<String, Arc<Semaphore>>)>,
    /// 集群共享密钥：带着它转发来的请求已在第一个节点计过（见 [`crate::service::trusted_forward`]）。
    cluster_secret: Option<Arc<str>>,
}

impl TenantLimitLayer {
    /// `rate` 为 (每秒补充, 突发) 的令牌桶参数；`max_in_flight` 为每个租户的在途请求上限。
    pub fn new(rate: Option<(u32, u32)>, max_in_flight: Option<usize>) -> Self {
        Self {
            cluster_secret: None,
            rate: rate.map(|(r, b)| Arc::new(RateLimiter::new(r, b))),
            concurrency: max_in_flight.map(|n| {
                let cache = moka::sync::Cache::builder()
                    .max_capacity(MAX_KEYS)
                    .time_to_idle(KEY_IDLE)
                    .build();
                (n.max(1), cache)
            }),
        }
    }

    pub fn with_cluster_secret(mut self, secret: Option<Arc<str>>) -> Self {
        self.cluster_secret = secret.filter(|s| !s.is_empty());
        self
    }
}

impl<S> Layer<S> for TenantLimitLayer {
    type Service = TenantLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TenantLimit {
            inner,
            limits: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TenantLimit<S> {
    inner: S,
    limits: TenantLimitLayer,
}

/// 请求所属的租户：认证过的主体，否则为来源 IP。
fn tenant_of<B>(req: &http::Request<B>) -> String {
    tenant_key(
        req.extensions().get::<scion_auth::Principal>(),
        req.extensions()
            .get::<tonic::transport::server::TcpConnectInfo>()
            .and_then(|c| c.remote_addr())
            .map(|a| a.ip()),
    )
}

/// The tenant key: `sub:<subject>` when authenticated, else `ip:<addr>`, else `anonymous`.
/// Stream budgets ([`crate::streams`]) use the same key.
pub fn tenant_key(principal: Option<&scion_auth::Principal>, addr: Option<std::net::IpAddr>) -> String {
    match (principal, addr) {
        (Some(p), _) if p.authenticated => format!("sub:{}", p.subject),
        (_, Some(ip)) => format!("ip:{ip}"),
        _ => "anonymous".to_owned(),
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for TenantLimit<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        // 健康检查不限；本集群转发来的请求已在第一个节点计过（调用方自带的转发头照常计数）
        let forwarded = crate::service::trusted_forward(
            req.headers().get(crate::FORWARDED_HEADER).map(|v| v.as_bytes()),
            self.limits.cluster_secret.as_deref(),
        );
        if req.uri().path().starts_with("/grpc.health.v1.")
            || forwarded
            || (self.limits.rate.is_none() && self.limits.concurrency.is_none())
        {
            return Box::pin(inner.call(req));
        }
        let tenant = tenant_of(&req);
        let reject = |reason: &str, message: String, wait: Duration| {
            scion_obs::metrics().reject(reason);
            let s = status::rate_limited(&message, wait);
            Box::pin(std::future::ready(Ok(s.into_http()))) as Self::Future
        };
        if let Some(Err(wait)) = self.limits.rate.as_ref().map(|r| r.check(&tenant)) {
            return reject(
                reason::TENANT_RATE,
                format!("request rate limit for {tenant} exceeded"),
                wait,
            );
        }
        let permit = match &self.limits.concurrency {
            Some((max, sems)) => {
                let sem = sems.get_with_by_ref(&tenant, || Arc::new(Semaphore::new(*max)));
                match sem.try_acquire_owned() {
                    Ok(p) => Some(p),
                    Err(_) => {
                        return reject(
                            reason::TENANT_CONCURRENCY,
                            format!("too many requests in flight for {tenant}"),
                            Duration::from_millis(100),
                        );
                    }
                }
            }
            None => None,
        };
        let fut = inner.call(req);
        Box::pin(async move {
            let resp = fut.await;
            drop(permit);
            resp
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tokio::sync::Notify;

    #[test]
    fn token_bucket_refills_over_time_per_key() {
        let l = RateLimiter::new(10, 2);
        let t0 = Instant::now();
        assert!(l.check_at("a", t0).is_ok());
        assert!(l.check_at("a", t0).is_ok());
        let wait = l.check_at("a", t0).expect_err("burst used up");
        assert!(wait > Duration::ZERO && wait <= Duration::from_millis(100), "{wait:?}");
        // 其它仓库互不影响
        assert!(l.check_at("b", t0).is_ok());
        // 100 ms 补充 1 个
        assert!(l.check_at("a", t0 + Duration::from_millis(100)).is_ok());
        assert!(l.check_at("a", t0 + Duration::from_millis(100)).is_err());
        // 不超过 burst
        let later = t0 + Duration::from_secs(10);
        assert!(l.check_at("a", later).is_ok());
        assert!(l.check_at("a", later).is_ok());
        assert!(l.check_at("a", later).is_err());
        assert!(l.check("fresh").is_ok());
        // 参数下限为 1
        let tiny = RateLimiter::new(0, 0);
        assert!(tiny.check_at("x", t0).is_ok());
        assert!(tiny.check_at("x", t0).is_err());
    }

    #[test]
    fn admit_maps_to_rate_limited_status() {
        let l = RateLimiter::new(1, 1);
        assert!(RateLimiter::admit(Some(&l), "r").is_ok());
        let s = RateLimiter::admit(Some(&l), "r").expect_err("limited");
        assert_eq!(s.code(), tonic::Code::ResourceExhausted);
        assert!(RateLimiter::admit(None, "r").is_ok(), "no limiter configured");
    }

    /// 测试用服务：请求在 `gate` 放行前不返回。
    #[derive(Clone)]
    struct Slow(Arc<Notify>);

    impl Service<http::Request<()>> for Slow {
        type Response = http::Response<String>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let gate = self.0.clone();
            Box::pin(async move {
                gate.notified().await;
                Ok(http::Response::new("ok".to_string()))
            })
        }
    }

    fn request(path: &str) -> http::Request<()> {
        http::Request::builder().uri(path).body(()).expect("request")
    }

    fn as_user(mut req: http::Request<()>, subject: &str) -> http::Request<()> {
        req.extensions_mut()
            .insert(scion_auth::Principal::user(subject, vec![]));
        req
    }

    fn from_ip(mut req: http::Request<()>, ip: &str) -> http::Request<()> {
        let addr: std::net::SocketAddr = format!("{ip}:1234").parse().expect("addr");
        req.extensions_mut().insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(addr),
        });
        req
    }

    fn code_of(resp: &http::Response<String>) -> Option<tonic::Code> {
        Status::from_header_map(resp.headers()).map(|s| s.code())
    }

    #[test]
    fn tenants_are_subjects_or_source_addresses() {
        assert_eq!(tenant_of(&as_user(request("/x"), "alice")), "sub:alice");
        assert_eq!(tenant_of(&from_ip(request("/x"), "10.0.0.7")), "ip:10.0.0.7");
        let mut anon = from_ip(request("/x"), "10.0.0.8");
        anon.extensions_mut().insert(scion_auth::Principal::anonymous());
        assert_eq!(tenant_of(&anon), "ip:10.0.0.8", "anonymous principals count by address");
        assert_eq!(tenant_of(&request("/x")), "anonymous");
    }

    #[tokio::test]
    async fn tenant_rate_limits_each_caller_separately() {
        let gate = Arc::new(Notify::new());
        let mut svc = TenantLimitLayer::new(Some((1, 1)), None)
            .with_cluster_secret(Some(Arc::from("s3cret")))
            .layer(Slow(gate.clone()));
        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.expect("ready");
        let first = tokio::spawn(svc.call(as_user(request("/s.S/M"), "alice")));
        tokio::task::yield_now().await;
        let limited = svc.call(as_user(request("/s.S/M"), "alice")).await.expect("response");
        assert_eq!(code_of(&limited), Some(tonic::Code::ResourceExhausted));
        // 调用方自带的转发头不能绕过限流
        let mut spoofed = as_user(request("/s.S/M"), "alice");
        spoofed
            .headers_mut()
            .insert(crate::FORWARDED_HEADER, http::HeaderValue::from_static("1"));
        let limited = svc.call(spoofed).await.expect("response");
        assert_eq!(code_of(&limited), Some(tonic::Code::ResourceExhausted));
        // 别的调用者、健康检查和集群内转发不受影响
        let bob = tokio::spawn(svc.call(as_user(request("/s.S/M"), "bob")));
        let health = tokio::spawn(svc.call(as_user(request("/grpc.health.v1.Health/Check"), "alice")));
        let mut fwd = as_user(request("/s.S/M"), "alice");
        fwd.headers_mut()
            .insert(crate::FORWARDED_HEADER, http::HeaderValue::from_static("s3cret"));
        let forwarded = tokio::spawn(svc.call(fwd));
        for _ in 0..4 {
            tokio::task::yield_now().await;
            gate.notify_waiters();
        }
        for h in [first, bob, health, forwarded] {
            assert_eq!(h.await.expect("join").expect("ok").into_body(), "ok");
        }
    }

    #[tokio::test]
    async fn tenant_concurrency_is_capped_per_caller() {
        let gate = Arc::new(Notify::new());
        let mut svc = TenantLimitLayer::new(None, Some(1)).layer(Slow(gate.clone()));
        let first = tokio::spawn(svc.call(from_ip(request("/s.S/M"), "10.0.0.1")));
        tokio::task::yield_now().await;
        let busy = svc
            .call(from_ip(request("/s.S/M"), "10.0.0.1"))
            .await
            .expect("response");
        assert_eq!(code_of(&busy), Some(tonic::Code::ResourceExhausted));
        let other = tokio::spawn(svc.call(from_ip(request("/s.S/M"), "10.0.0.2")));
        for _ in 0..3 {
            tokio::task::yield_now().await;
            gate.notify_waiters();
        }
        assert_eq!(first.await.expect("join").expect("ok").into_body(), "ok");
        assert_eq!(other.await.expect("join").expect("ok").into_body(), "ok");
        // 名额释放后可以继续
        let next = tokio::spawn(svc.call(from_ip(request("/s.S/M"), "10.0.0.1")));
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(next.await.expect("join").expect("ok").into_body(), "ok");
        // 都不限制时直接放行
        let mut free = TenantLimitLayer::default().layer(Slow(gate.clone()));
        let h = tokio::spawn(free.call(request("/s.S/M")));
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(h.await.expect("join").expect("ok").into_body(), "ok");
    }

    #[tokio::test]
    async fn sheds_load_beyond_the_limit_but_not_health_checks() {
        let gate = Arc::new(Notify::new());
        let mut svc = ConcurrencyLimitLayer::new(1).layer(Slow(gate.clone()));
        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.expect("ready");
        let first = tokio::spawn(svc.call(request("/scion.v1.LandService/Land")));
        tokio::task::yield_now().await;
        // 第二个请求立即被拒绝
        let shed = svc.call(request("/scion.v1.LandService/Land")).await.expect("response");
        let status = Status::from_header_map(shed.headers()).expect("grpc status");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        // 健康检查不占名额
        let health = tokio::spawn(svc.call(request("/grpc.health.v1.Health/Check")));
        gate.notify_waiters();
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(first.await.expect("join").expect("ok").into_body(), "ok");
        assert_eq!(health.await.expect("join").expect("ok").into_body(), "ok");
        // 名额释放后可以继续
        let next = tokio::spawn(svc.call(request("/scion.v1.LandService/Land")));
        tokio::task::yield_now().await;
        gate.notify_waiters();
        assert_eq!(next.await.expect("join").expect("ok").into_body(), "ok");
    }
}
