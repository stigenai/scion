//! 请求的认证（tower 中间件）与各接口使用的鉴权辅助函数。
//!
//! [`AuthLayer`] 读取 `authorization: Bearer <token>`，认证出 [`Principal`] 放进请求的 extensions；
//! 令牌无效或过期时直接返回 UNAUTHENTICATED。没有令牌时是 anonymous，是否允许由各接口按策略判断。
//! 不需要身份的接口（健康检查、能力协商、换取令牌与 device login）跳过认证。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use scion_auth::{Action, AuthError, Guard, Principal, Request as AuthRequest};
use scion_proto::v1::ErrorReason;
use tonic::{Code, Status};
use tower::{Layer, Service};

use crate::status::with_reason;

/// 认证错误转成 gRPC 状态。
pub fn to_status(e: AuthError) -> Status {
    match e {
        AuthError::Missing => Status::unauthenticated("authentication required"),
        AuthError::Expired => with_reason(
            Code::Unauthenticated,
            ErrorReason::TokenExpired,
            "token expired",
            HashMap::new(),
        ),
        AuthError::Invalid(m) => Status::unauthenticated(m),
        AuthError::Denied {
            repo,
            action,
            reference,
            path,
        } => {
            let mut md = HashMap::from([
                ("repo".to_string(), repo.clone()),
                ("action".to_string(), action.to_string()),
            ]);
            if let Some(r) = reference {
                md.insert("ref".into(), r);
            }
            if let Some(p) = path {
                md.insert("path".into(), p);
            }
            with_reason(
                Code::PermissionDenied,
                ErrorReason::PermissionDenied,
                format!("permission denied: {action} on {repo}"),
                md,
            )
        }
        AuthError::Unavailable(m) => Status::unavailable(m),
        AuthError::BadRequest(m) => Status::invalid_argument(m),
    }
}

/// 请求方的身份（中间件放入；没有时视为 anonymous）。
pub fn principal<T>(req: &tonic::Request<T>) -> Principal {
    req.extensions()
        .get::<Principal>()
        .cloned()
        .unwrap_or_else(Principal::anonymous)
}

/// 检查一个请求，不允许时返回 PERMISSION_DENIED / UNAUTHENTICATED。
pub async fn authorize(guard: &Guard, p: &Principal, req: AuthRequest<'_>) -> Result<(), Status> {
    guard.authorize(p, &req).await.map_err(to_status)
}

/// 检查仓库中是否有某个权限（不论 ref 和路径的限制）。
pub async fn authorize_any(guard: &Guard, p: &Principal, repo: &str, action: Action) -> Result<(), Status> {
    guard.authorize_any(p, repo, action).await.map_err(to_status)
}

/// 不需要身份的接口。
fn public(path: &str) -> bool {
    path.starts_with("/grpc.health.v1.")
        || path == "/scion.v1.ServerInfoService/GetServerInfo"
        || path == "/scion.v1.AuthService/ExchangeToken"
        || path == "/scion.v1.AuthService/StartDeviceLogin"
        || path == "/scion.v1.AuthService/PollDeviceLogin"
}

fn bearer(headers: &http::HeaderMap) -> Option<String> {
    bearer_value(headers.get(http::header::AUTHORIZATION)?.to_str().ok()?)
}

fn bearer_value(v: &str) -> Option<String> {
    let (scheme, token) = v.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim().to_owned())
}

/// 请求的 bearer 令牌（与 [`AuthLayer`] 取的相同），长连接用它重新认证。
pub fn request_bearer<T>(req: &tonic::Request<T>) -> Option<String> {
    bearer_value(req.metadata().get("authorization")?.to_str().ok()?)
}

#[derive(Clone)]
pub struct AuthLayer {
    guard: Arc<Guard>,
}

impl AuthLayer {
    pub fn new(guard: Arc<Guard>) -> Self {
        Self { guard }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            guard: self.guard.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    guard: Arc<Guard>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for AuthService<S>
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

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let guard = self.guard.clone();
        Box::pin(async move {
            if public(req.uri().path()) {
                return inner.call(req).await;
            }
            match guard.authenticate(bearer(req.headers()).as_deref()).await {
                Ok(p) => {
                    req.extensions_mut().insert(p);
                    inner.call(req).await
                }
                Err(e) => Ok(to_status(e).into_http()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tonic_types::StatusExt;

    /// 测试用服务：把认证出的身份写进响应头。
    #[derive(Clone)]
    struct Echo;

    impl Service<http::Request<()>> for Echo {
        type Response = http::Response<String>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Infallible>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<()>) -> Self::Future {
            let who = req
                .extensions()
                .get::<Principal>()
                .map_or_else(|| "none".to_string(), |p| p.subject.clone());
            Box::pin(async move { Ok(http::Response::new(who)) })
        }
    }

    fn request(path: &str, auth: Option<&str>) -> http::Request<()> {
        let mut b = http::Request::builder().uri(path);
        if let Some(a) = auth {
            b = b.header("authorization", a);
        }
        b.body(()).expect("request")
    }

    async fn guard() -> (Arc<Guard>, String) {
        let meta = Arc::new(scion_store::MemStore::new());
        let g = Guard::build(
            scion_auth::GuardConfig {
                required: true,
                ..Default::default()
            },
            meta,
            Arc::new(|_: &str| None),
        )
        .expect("guard");
        let (pat, _) = g.create_token("pat", "alice", vec![], "", None).await.expect("pat");
        (g, pat)
    }

    #[tokio::test]
    async fn layer_authenticates_or_rejects() {
        let (g, pat) = guard().await;
        let mut svc = AuthLayer::new(g).layer(Echo);
        let ok = svc
            .call(request("/scion.v1.RefService/ListRefs", Some(&format!("Bearer {pat}"))))
            .await
            .expect("resp");
        assert_eq!(ok.into_body(), "alice");
        let anon = svc
            .call(request("/scion.v1.RefService/ListRefs", None))
            .await
            .expect("resp");
        assert_eq!(anon.into_body(), "anonymous");
        let bad = svc
            .call(request("/scion.v1.RefService/ListRefs", Some("Bearer scn_pat_nope")))
            .await
            .expect("resp");
        let s = Status::from_header_map(bad.headers()).expect("status");
        assert_eq!(s.code(), Code::Unauthenticated);
        // 公开接口不认证（即使令牌无效）
        let public = svc
            .call(request(
                "/scion.v1.AuthService/ExchangeToken",
                Some("Bearer scn_pat_nope"),
            ))
            .await
            .expect("resp");
        assert_eq!(public.into_body(), "none");
        // 非 Bearer 方案视为没有令牌
        let basic = svc
            .call(request("/scion.v1.RefService/ListRefs", Some("Basic abc")))
            .await
            .expect("resp");
        assert_eq!(basic.into_body(), "anonymous");
    }

    #[test]
    fn errors_map_to_statuses() {
        assert_eq!(to_status(AuthError::Missing).code(), Code::Unauthenticated);
        let expired = to_status(AuthError::Expired);
        assert_eq!(expired.code(), Code::Unauthenticated);
        assert_eq!(
            expired.get_error_details().error_info().map(|i| i.reason.clone()),
            Some("TOKEN_EXPIRED".into())
        );
        let denied = to_status(AuthError::Denied {
            repo: "mono".into(),
            action: "land",
            reference: Some("main".into()),
            path: Some("src".into()),
        });
        assert_eq!(denied.code(), Code::PermissionDenied);
        let info = denied.get_error_details().error_info().cloned().expect("info");
        assert_eq!(info.reason, "PERMISSION_DENIED");
        assert_eq!(
            (info.metadata["ref"].as_str(), info.metadata["path"].as_str()),
            ("main", "src")
        );
        assert_eq!(to_status(AuthError::Invalid("x".into())).code(), Code::Unauthenticated);
        assert_eq!(to_status(AuthError::Unavailable("x".into())).code(), Code::Unavailable);
        assert_eq!(
            to_status(AuthError::BadRequest("x".into())).code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn principal_defaults_to_anonymous() {
        let mut req = tonic::Request::new(());
        assert!(!principal(&req).authenticated);
        req.extensions_mut().insert(Principal::user("bob", vec![]));
        assert_eq!(principal(&req).subject, "bob");
    }
}
