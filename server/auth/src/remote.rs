//! 调用外部服务的认证与鉴权：
//! - [`Introspection`]：RFC 7662 令牌内省，校验外部签发的不透明令牌（例如 starhub 的 access token）；
//! - [`AuthzWebhook`]：外部鉴权回调，请求和响应格式与 OPA 的 REST 接口相同，可以直接接 OPA，也可以由
//!   其它系统（例如按 rebac 判断权限的 starhub）实现同样的接口。
//!
//! 两者的结果都在本地按 TTL 缓存（拒绝的结果缓存更短），避免每个请求都调用外部服务。

use std::time::Duration;

use moka::future::Cache;
use serde_json::{Value, json};

use crate::config::{ClientAuth, IntrospectionConfig, WebhookConfig};
use crate::external::{VerifyError, principal_from_claims};
use crate::policy::{Principal, Request};

/// 拒绝 / 无效结果的缓存时间。
const NEGATIVE_TTL: Duration = Duration::from_secs(5);

fn apply_auth(
    req: reqwest::RequestBuilder,
    auth: &ClientAuth,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<reqwest::RequestBuilder, String> {
    Ok(match auth {
        ClientAuth::None => req,
        ClientAuth::Bearer(s) => req.bearer_auth(s.resolve(lookup).map_err(|e| e.to_string())?),
        ClientAuth::Basic { client_id, secret } => {
            let pw = if secret == &crate::config::Secret::default() {
                None
            } else {
                Some(secret.resolve(lookup).map_err(|e| e.to_string())?)
            };
            req.basic_auth(client_id, pw)
        }
    })
}

/// 读环境变量的函数（测试可替换）。
pub type Lookup = std::sync::Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

pub fn env_lookup() -> Lookup {
    std::sync::Arc::new(|k: &str| std::env::var(k).ok())
}

/// RFC 7662 令牌内省。
pub struct Introspection {
    config: IntrospectionConfig,
    http: reqwest::Client,
    lookup: Lookup,
    /// key 为令牌的 SHA-256（不在内存里保存令牌本身）。
    cache: Cache<[u8; 32], Option<Principal>>,
}

impl Introspection {
    pub fn new(config: IntrospectionConfig, http: reqwest::Client, lookup: Lookup) -> Self {
        let ttl = config.cache_ttl;
        Self {
            config,
            http,
            lookup,
            cache: Cache::builder()
                .max_capacity(100_000)
                .expire_after(crate::remote::Ttl { positive: ttl })
                .build(),
        }
    }

    async fn call(&self, token: &str) -> Result<Option<Principal>, VerifyError> {
        let req = self.http.post(&self.config.url).form(&[("token", token)]);
        let req = apply_auth(req, &self.config.auth, &*self.lookup).map_err(VerifyError::Unavailable)?;
        let resp = req
            .send()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("introspection: {e}")))?;
        if !resp.status().is_success() {
            return Err(VerifyError::Unavailable(format!(
                "introspection: HTTP {}",
                resp.status()
            )));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("introspection response: {e}")))?;
        if body.get("active").and_then(Value::as_bool) != Some(true) {
            return Ok(None);
        }
        // RFC 7662 的身份字段是 username，也常用 sub
        let subject_claim = if body.get(&self.config.subject_claim).is_some() {
            self.config.subject_claim.as_str()
        } else {
            "sub"
        };
        principal_from_claims(&body, subject_claim, self.config.groups_claim.as_deref(), None).map(Some)
    }

    /// How long the cache keeps an active answer. A token that `exchange` derives must not outlive it.
    pub fn cache_ttl(&self) -> Duration {
        self.config.cache_ttl
    }

    /// 校验令牌：有效时返回身份，无效返回 `Ok(None)`。
    ///
    /// After a revoke, this call can still accept the token for `cache_ttl` (default 60 s), but not past its `exp`.
    /// An active answer with an `exp` that is not in the future gives `Ok(None)`.
    /// With `cache_ttl = "0s"` the cache is off.
    pub async fn verify(&self, token: &str) -> Result<Option<Principal>, VerifyError> {
        let key = crate::token::hash_opaque(token);
        let cached = !self.config.cache_ttl.is_zero();
        if cached && let Some(hit) = self.cache.get(&key).await {
            return Ok(hit);
        }
        let now = crate::guard::now_ms();
        let r = self
            .call(token)
            .await?
            .filter(|p| p.expires_at_ms.is_none_or(|e| e > now));
        if cached {
            self.cache.insert(key, r.clone()).await;
        }
        Ok(r)
    }
}

/// 允许的结果缓存 `positive`，但不超过令牌的 `exp`；拒绝的缓存 [`NEGATIVE_TTL`]。
struct Ttl {
    positive: Duration,
}

impl<K> moka::Expiry<K, Option<Principal>> for Ttl {
    fn expire_after_create(&self, _: &K, v: &Option<Principal>, _: std::time::Instant) -> Option<Duration> {
        let Some(p) = v else {
            return Some(NEGATIVE_TTL);
        };
        let left = p
            .expires_at_ms
            .map(|e| Duration::from_millis(u64::try_from(e - crate::guard::now_ms()).unwrap_or(0)));
        Some(left.map_or(self.positive, |l| l.min(self.positive)))
    }
}

struct BoolTtl {
    positive: Duration,
}

impl<K> moka::Expiry<K, bool> for BoolTtl {
    fn expire_after_create(&self, _: &K, v: &bool, _: std::time::Instant) -> Option<Duration> {
        Some(if *v { self.positive } else { NEGATIVE_TTL })
    }
}

/// OPA 兼容的外部鉴权回调。
///
/// 请求：`POST {"input": {"subject", "groups", "authenticated", "repo", "action", "ref", "path"}}`；
/// 响应：`{"result": true}` 或 `{"result": {"allow": true}}`，其余（包括没有 `result`）视为拒绝。
pub struct AuthzWebhook {
    config: WebhookConfig,
    http: reqwest::Client,
    lookup: Lookup,
    cache: Cache<String, bool>,
}

impl AuthzWebhook {
    pub fn new(config: WebhookConfig, http: reqwest::Client, lookup: Lookup) -> Self {
        let ttl = config.cache_ttl;
        Self {
            config,
            http,
            lookup,
            cache: Cache::builder()
                .max_capacity(100_000)
                .expire_after(BoolTtl { positive: ttl })
                .build(),
        }
    }

    /// 回调不可用时是否放行给 ACL 判断（`on_error = "open"`）。
    pub fn fail_open(&self) -> bool {
        self.config.fail_open
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Whether a `false` answer denies rather than passing to the next decider.
    pub fn denies(&self) -> bool {
        self.config.denies
    }

    /// Whether the webhook is asked about `repo` (its `repos` patterns; none means every one).
    pub fn covers(&self, repo: &str) -> bool {
        self.config.repos.is_empty() || self.config.repos.iter().any(|p| crate::policy::glob(p, repo))
    }

    fn input(p: &Principal, req: &Request<'_>) -> Value {
        json!({
            "input": {
                "subject": p.subject,
                "groups": p.groups,
                "authenticated": p.authenticated,
                "repo": req.repo,
                "action": req.action.as_str(),
                "ref": req.reference,
                "path": req.path,
            }
        })
    }

    async fn call(&self, body: &Value) -> Result<bool, String> {
        let req = self.http.post(&self.config.url).timeout(self.config.timeout).json(body);
        let resp = apply_auth(req, &self.config.auth, &*self.lookup)?
            .send()
            .await
            .map_err(|e| format!("authz webhook: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("authz webhook: HTTP {}", resp.status()));
        }
        let v: Value = resp.json().await.map_err(|e| format!("authz webhook response: {e}"))?;
        Ok(match v.get("result") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Object(o)) => o.get("allow").and_then(Value::as_bool).unwrap_or(false),
            _ => false,
        })
    }

    /// 是否允许。回调失败时返回错误，由调用方按 `fail_open` 处理。
    pub async fn allows(&self, p: &Principal, req: &Request<'_>) -> Result<bool, String> {
        let body = Self::input(p, req);
        let key = body.to_string();
        if let Some(hit) = self.cache.get(&key).await {
            return Ok(hit);
        }
        let allowed = self.call(&body).await?;
        self.cache.insert(key, allowed).await;
        Ok(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;
    use crate::policy::Action;
    use crate::testing::FakeIdp;

    fn introspection(url: String, auth: ClientAuth) -> Introspection {
        Introspection::new(
            IntrospectionConfig {
                url,
                auth,
                cache_ttl: Duration::from_secs(60),
                subject_claim: "username".into(),
                groups_claim: Some("groups".into()),
            },
            reqwest::Client::new(),
            std::sync::Arc::new(|k: &str| (k == "SECRET").then(|| "s3cret".to_string())),
        )
    }

    #[tokio::test]
    async fn introspection_validates_and_caches() {
        let idp = FakeIdp::start().await;
        let basic = ClientAuth::Basic {
            client_id: "scion".into(),
            secret: Secret {
                env: Some("SECRET".into()),
                ..Secret::default()
            },
        };
        let i = introspection(idp.url("/introspect"), basic);
        let p = i.verify("good-token").await.expect("ok").expect("active");
        assert_eq!(
            (p.subject.as_str(), p.groups.as_slice()),
            ("alice", &["eng".to_string()][..])
        );
        assert_eq!(
            i.verify("good-token").await.expect("cached").map(|p| p.subject),
            Some("alice".into())
        );
        assert_eq!(idp.calls("/introspect"), 1, "second lookup served from the cache");
        assert_eq!(i.verify("revoked").await, Ok(None));
        // sub 作为身份字段的提供方
        assert_eq!(
            i.verify("sub-only").await.expect("ok").map(|p| p.subject),
            Some("bob".into())
        );
        // 凭据不对（提供方返回 401）、提供方不可达
        let bad = introspection(
            idp.url("/introspect"),
            ClientAuth::Bearer(Secret {
                value: Some("x".into()),
                ..Secret::default()
            }),
        );
        assert!(matches!(
            bad.verify("good-token").await,
            Err(VerifyError::Unavailable(_))
        ));
        let down = introspection("http://127.0.0.1:1/introspect".into(), ClientAuth::None);
        assert!(matches!(down.verify("t").await, Err(VerifyError::Unavailable(_))));
        let not_json = introspection(idp.url("/not-json"), ClientAuth::None);
        assert!(matches!(not_json.verify("t").await, Err(VerifyError::Unavailable(_))));
        let missing_secret = introspection(
            idp.url("/introspect"),
            ClientAuth::Bearer(Secret {
                env: Some("UNSET".into()),
                ..Secret::default()
            }),
        );
        assert!(missing_secret.verify("t").await.is_err());
    }

    fn webhook(url: String, fail_open: bool) -> AuthzWebhook {
        AuthzWebhook::new(
            WebhookConfig {
                url,
                auth: ClientAuth::Bearer(Secret {
                    value: Some("opa".into()),
                    ..Secret::default()
                }),
                cache_ttl: Duration::from_secs(30),
                timeout: Duration::from_secs(2),
                fail_open,
                ..WebhookConfig::default()
            },
            reqwest::Client::new(),
            env_lookup(),
        )
    }

    #[tokio::test]
    async fn webhook_decisions_follow_opa_shapes() {
        let idp = FakeIdp::start().await;
        let w = webhook(idp.url("/authz"), false);
        assert!(!w.fail_open());
        let alice = Principal::user("alice", vec![]);
        let read = Request::repo("models/a", Action::Read);
        assert_eq!(w.allows(&alice, &read).await, Ok(true), "result: true");
        assert_eq!(w.allows(&alice, &read).await, Ok(true));
        assert_eq!(idp.calls("/authz"), 1, "cached");
        let write = Request::repo("models/a", Action::Write);
        assert_eq!(w.allows(&alice, &write).await, Ok(false), "result: {{allow: false}}");
        let anon_public = Request::repo("public", Action::Read);
        assert_eq!(
            w.allows(&Principal::anonymous(), &anon_public).await,
            Ok(true),
            "result: {{allow: true}}"
        );
        assert_eq!(w.allows(&Principal::anonymous(), &write).await, Ok(false), "no result");
        let down = webhook("http://127.0.0.1:1/authz".into(), true);
        assert!(down.fail_open());
        assert!(down.allows(&alice, &read).await.is_err());
        let err = webhook(idp.url("/status-500"), false);
        assert!(err.allows(&alice, &read).await.is_err());
        let not_json = webhook(idp.url("/not-json"), false);
        assert!(not_json.allows(&alice, &read).await.is_err());
    }
}
