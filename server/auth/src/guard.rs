//! [`Guard`]：认证与鉴权的入口，服务端各接口通过它判断请求方是谁、能做什么。
//!
//! **认证**（`authorization: Bearer <token>`）：
//! - 没有令牌：anonymous；
//! - `scn_` 开头：不透明令牌（PAT / service token），查 `tokens` 表；
//! - JWT：`iss` 为本服务时用自己的公钥验证；否则交给配置的外部签发方（JWKS 或 HS256）；
//! - 其它：配置了 RFC 7662 内省时交给内省接口。
//!
//! **Authorization** runs a chain of deciders (docs/design/authorization.md): each allows, denies
//! or abstains, and the first that allows or denies settles the request; if all abstain it is
//! denied. The chain is the ACL policy, then the token's own grants, then the authorization
//! callback. With no policy saved and no callback, the built-in default policy decides. Without
//! required authentication everything is allowed.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use scion_store::{MetaStore, TokenRecord};

use crate::config::{AuthConfig, ChainEntry};
use crate::external::{ExternalIssuer, VerifyError};
use crate::oidc::{DevicePoll, DeviceStart, Oidc};
use crate::policy::{Action, Policy, Principal, Request, grants_allow};
use crate::remote::{AuthzWebhook, Introspection, Lookup};
use crate::token::{Issuer, OPAQUE_PREFIX, hash_opaque, new_opaque, peek};

/// 访问控制策略在设置表中的 key。
pub const POLICY_KEY: &str = "acl";

/// device login 得到的刷新令牌的有效期。
const REFRESH_TTL: Duration = Duration::from_secs(30 * 86_400);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("authentication required")]
    Missing,
    #[error("token expired")]
    Expired,
    #[error("invalid credentials: {0}")]
    Invalid(String),
    #[error("permission denied: {action} on {repo}")]
    Denied {
        repo: String,
        action: &'static str,
        reference: Option<String>,
        path: Option<String>,
    },
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    BadRequest(String),
}

impl From<VerifyError> for AuthError {
    fn from(e: VerifyError) -> Self {
        match e {
            VerifyError::Expired => AuthError::Expired,
            VerifyError::Invalid(m) => AuthError::Invalid(m),
            VerifyError::Unavailable(m) => AuthError::Unavailable(m),
        }
    }
}

impl From<scion_store::Error> for AuthError {
    fn from(e: scion_store::Error) -> Self {
        AuthError::Unavailable(e.to_string())
    }
}

/// 构建 [`Guard`] 的参数。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GuardConfig {
    /// 是否要求认证。
    pub required: bool,
    pub auth: AuthConfig,
    /// 自签令牌的私钥（PKCS#8 PEM）；不提供时不能签发访问令牌（ExchangeToken、device login 不可用）。
    pub signing_key_pem: Option<String>,
}

/// What one decider says about a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    /// No opinion: the next decider is asked.
    Abstain,
}

/// The deciders [`Guard::check`] asks, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decider {
    /// The saved ACL policy, or the built-in default when none is saved and no callback is set.
    Policy,
    /// Grants carried by the token (`repos_claim`).
    Grants,
    /// The webhook at this index of `Guard::webhooks`.
    Webhook(usize),
    /// Organization roles.
    Orgs,
    /// Personal namespaces.
    Personal,
}

/// How long organization memberships and visibility are cached; changes made on another node take
/// effect within this (changes made through this node at once).
const ORG_CACHE_TTL: Duration = Duration::from_secs(30);

/// Organization data the chain reads, cached.
struct OrgCache {
    /// subject to its `org:ORG:ROLE` groups.
    memberships: moka::future::Cache<String, Arc<Vec<String>>>,
    /// organization to its visibility (None: no such organization).
    visibility: moka::future::Cache<String, Option<String>>,
}

impl OrgCache {
    fn new() -> Self {
        Self {
            memberships: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(ORG_CACHE_TTL)
                .build(),
            visibility: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(ORG_CACHE_TTL)
                .build(),
        }
    }
}

pub struct Guard {
    org_creation_admin_only: bool,
    require_policy: bool,
    required: bool,
    chain: Vec<Decider>,
    meta: Option<Arc<dyn MetaStore>>,
    issuer: Option<Issuer>,
    externals: Vec<ExternalIssuer>,
    introspection: Option<Introspection>,
    webhooks: Vec<AuthzWebhook>,
    /// Set when `orgs` is in the chain.
    orgs: Option<OrgCache>,
    oidc: Option<Oidc>,
    github: Option<crate::github::GitHub>,
    /// GitHub organization and team memberships as groups.
    teams: Option<crate::github::Teams>,
    policy: RwLock<PolicyState>,
}

/// 内存中的访问控制策略。
#[derive(Debug, Clone)]
enum PolicyState {
    /// 还没从元数据库加载成功：拒绝一切（返回 Unavailable），不能退回到默认放行。
    Unloaded,
    /// 元数据库里没有策略：没有鉴权回调时按 [`DEFAULT_POLICY`](crate::policy::DEFAULT_POLICY) 执行。
    Unset,
    /// 显式保存的策略（包括空策略：没有规则命中即拒绝）。
    Set(Arc<Policy>),
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// The client for calls out (JWKS, introspection, the authorization callback, OIDC).
fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| {
            format!("HTTP client: {e} (are the system's CA certificates installed? on Debian or Ubuntu: apt-get install ca-certificates)")
        })
}

impl Guard {
    /// 不要求认证、一切允许（Lite 模式、测试）。
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::unconfigured())
    }

    /// 要求认证但无法验证任何令牌：拒绝一切（认证配置无效时的兜底，宁可拒绝也不放行）。
    pub fn deny_all() -> Arc<Self> {
        Arc::new(Self {
            required: true,
            policy: RwLock::new(PolicyState::Set(Arc::new(Policy::default()))),
            ..Self::unconfigured()
        })
    }

    fn unconfigured() -> Self {
        Self {
            org_creation_admin_only: false,
            require_policy: false,
            required: false,
            chain: vec![Decider::Policy, Decider::Grants],
            meta: None,
            issuer: None,
            externals: Vec::new(),
            introspection: None,
            webhooks: Vec::new(),
            orgs: None,
            oidc: None,
            github: None,
            teams: None,
            policy: RwLock::new(PolicyState::Unset),
        }
    }

    pub fn build(config: GuardConfig, meta: Arc<dyn MetaStore>, lookup: Lookup) -> Result<Arc<Self>, String> {
        // Only external issuers, introspection, the authorization callback and OIDC call out, so
        // only they need an HTTP client: building one needs the system's CA certificates, which a
        // minimal Linux may not have, and a server that calls nobody should still start there
        let a = &config.auth;
        if !config.required && (a.authorization.require_policy || a.authorization.org_creation_admin_only) {
            return Err("authorization.require_policy and org_creation_admin_only require authentication".into());
        }
        let http = if a.issuers.is_empty()
            && a.introspection.is_none()
            && a.webhooks().next().is_none()
            && a.oidc.is_none()
            && a.github.is_none()
        {
            None
        } else {
            Some(http_client()?)
        };
        let http = || http.clone().expect("built when something calls out");
        let webhooks: Vec<AuthzWebhook> = config
            .auth
            .webhooks()
            .map(|c| AuthzWebhook::new(c.clone(), http(), lookup.clone()))
            .collect();
        let chain = config
            .auth
            .chain()
            .into_iter()
            .map(|e| match e {
                ChainEntry::Policy => Ok(Decider::Policy),
                ChainEntry::Grants => Ok(Decider::Grants),
                ChainEntry::Orgs => Ok(Decider::Orgs),
                ChainEntry::Personal => Ok(Decider::Personal),
                ChainEntry::Webhook(name) => webhooks
                    .iter()
                    .position(|w| w.name() == name)
                    .map(Decider::Webhook)
                    .ok_or_else(|| format!("authorization.chain names an unknown webhook {name:?}")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ttl = i64::try_from(config.auth.token.ttl().as_secs()).unwrap_or(900);
        let issuer = config
            .signing_key_pem
            .as_deref()
            .map(|pem| Issuer::from_pem(pem, &config.auth.token.issuer, ttl))
            .transpose()
            .map_err(|e| e.to_string())?;
        let externals = config
            .auth
            .issuers
            .into_iter()
            .map(|i| ExternalIssuer::new(i, http(), &*lookup).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let orgs = chain.contains(&Decider::Orgs).then(OrgCache::new);
        let teams = match &config.auth.github {
            Some(g) => crate::github::Teams::new(g, http(), &*lookup)?,
            None => None,
        };
        Ok(Arc::new(Self {
            required: config.required,
            org_creation_admin_only: config.auth.authorization.org_creation_admin_only,
            require_policy: config.auth.authorization.require_policy,
            orgs,
            chain,
            meta: Some(meta),
            issuer,
            externals,
            introspection: config
                .auth
                .introspection
                .map(|c| Introspection::new(c, http(), lookup.clone())),
            webhooks,
            teams,
            github: config.auth.github.map(|c| crate::github::GitHub::new(c, http())),
            oidc: config.auth.oidc.map(|c| Oidc::new(c, http(), lookup)),
            policy: RwLock::new(PolicyState::Unloaded),
        }))
    }

    pub fn required(&self) -> bool {
        self.required
    }

    /// Whether the chain includes `orgs` (and OrgService is available).
    pub fn orgs_enabled(&self) -> bool {
        self.orgs.is_some()
    }

    /// Whether organization creation is restricted to system administrators.
    pub fn org_creation_admin_only(&self) -> bool {
        self.org_creation_admin_only
    }

    /// 能否签发访问令牌。
    pub fn can_issue(&self) -> bool {
        self.issuer.is_some()
    }

    pub fn device_login(&self) -> bool {
        (self.oidc.is_some() || self.github.is_some()) && self.issuer.is_some()
    }

    fn meta(&self) -> Result<&Arc<dyn MetaStore>, AuthError> {
        self.meta
            .as_ref()
            .ok_or_else(|| AuthError::Unavailable("authentication is not configured".into()))
    }

    // ---------- 认证 ----------

    async fn opaque(&self, token: &str, allow_refresh: bool) -> Result<Principal, AuthError> {
        Ok(self.opaque_record(token, allow_refresh).await?.0)
    }

    /// Like [`Self::opaque`], and also returns the token record id and expiry that bind an exchanged JWT.
    async fn opaque_record(
        &self,
        token: &str,
        allow_refresh: bool,
    ) -> Result<(Principal, String, Option<i64>), AuthError> {
        let rec = self
            .meta()?
            .get_token(&hash_opaque(token))
            .await?
            .ok_or_else(|| AuthError::Invalid("unknown token".into()))?;
        if rec.expires_at_ms.is_some_and(|e| e <= now_ms()) {
            return Err(AuthError::Expired);
        }
        if rec.kind == "refresh" && !allow_refresh {
            return Err(AuthError::Invalid(
                "refresh tokens can only be exchanged for an access token".into(),
            ));
        }
        let mut p = Principal::user(rec.subject, rec.groups);
        p.expires_at_ms = rec.expires_at_ms;
        Ok((p, rec.id, rec.expires_at_ms))
    }

    async fn jwt(&self, token: &str, allow_own: bool) -> Result<Principal, AuthError> {
        let iss = peek(token).and_then(|(iss, _)| iss);
        if let (Some(own), Some(iss)) = (&self.issuer, &iss) {
            if own.issuer() == iss {
                if !allow_own {
                    return Err(AuthError::Invalid("access tokens cannot be exchanged".into()));
                }
                let c = own.verify(token).map_err(VerifyError::from)?;
                let mut expires_at_ms = c.exp * 1000;
                if let Some(sid) = &c.sid {
                    // Fail closed: a missing or expired source record, or a store error, refuses the JWT.
                    // One primary-key read per request; there is no cache, so the window is 0 s.
                    let rec = self
                        .meta()?
                        .get_token_by_id(sid)
                        .await?
                        .ok_or_else(|| AuthError::Invalid("source token revoked".into()))?;
                    if rec.subject != c.sub {
                        return Err(AuthError::Invalid("source token does not match the subject".into()));
                    }
                    if let Some(src) = rec.expires_at_ms {
                        if src <= now_ms() {
                            return Err(AuthError::Expired);
                        }
                        expires_at_ms = expires_at_ms.min(src);
                    }
                }
                let mut p = Principal::user(c.sub, c.groups);
                p.expires_at_ms = Some(expires_at_ms);
                return Ok(p);
            }
        }
        match self.externals.iter().find(|e| Some(e.issuer()) == iss.as_deref()) {
            Some(e) => Ok(e.verify(token).await?),
            None => self.introspect(token).await,
        }
    }

    async fn introspect(&self, token: &str) -> Result<Principal, AuthError> {
        match &self.introspection {
            Some(i) => i
                .verify(token)
                .await?
                .ok_or_else(|| AuthError::Invalid("token is not active".into())),
            None => Err(AuthError::Invalid("token is not issued by a trusted issuer".into())),
        }
    }

    /// 认证一个请求。没有令牌时为 anonymous（是否允许由鉴权决定）。
    pub async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let Some(token) = bearer.map(str::trim).filter(|t| !t.is_empty()) else {
            return Ok(Principal::anonymous());
        };
        if !self.required && self.meta.is_none() {
            return Ok(Principal::anonymous());
        }
        if token.starts_with(OPAQUE_PREFIX) {
            self.opaque(token, false).await
        } else if token.split('.').count() == 3 {
            self.jwt(token, true).await
        } else {
            self.introspect(token).await
        }
    }

    // ---------- 令牌 ----------

    fn issue(&self, p: &Principal, source: Option<(&str, Option<i64>)>) -> Result<(String, i64), AuthError> {
        let issuer = self.issuer.as_ref().ok_or_else(|| {
            AuthError::Unavailable("this server does not issue access tokens (no signing key)".into())
        })?;
        Ok(issuer.issue(&p.subject, &p.groups, now_ms() / 1000, source))
    }

    /// 用长期凭据换取访问令牌：(令牌, 过期时间, 身份)。
    pub async fn exchange(&self, subject_token: &str) -> Result<(String, i64, String), AuthError> {
        let t = subject_token.trim();
        if t.is_empty() {
            return Err(AuthError::BadRequest("subject_token is required".into()));
        }
        let mut source = None;
        let p = if t.starts_with(OPAQUE_PREFIX) {
            let (p, id, exp) = self.opaque_record(t, true).await?;
            source = Some((id, exp));
            p
        } else if t.split('.').count() == 3 {
            self.jwt(t, false).await?
        } else {
            self.introspect(t).await?
        };
        let (token, exp) = self.issue(&p, source.as_ref().map(|(id, e)| (id.as_str(), *e)))?;
        Ok((token, exp, p.subject))
    }

    /// 创建不透明令牌，返回令牌（只出现这一次）与记录。
    pub async fn create_token(
        &self,
        kind: &str,
        subject: &str,
        groups: Vec<String>,
        description: &str,
        ttl: Option<Duration>,
    ) -> Result<(String, TokenRecord), AuthError> {
        if subject.trim().is_empty() {
            return Err(AuthError::BadRequest("subject is required".into()));
        }
        if groups.iter().any(|g| g.is_empty() || g.contains('\n')) {
            return Err(AuthError::BadRequest(
                "group names must be non-empty single lines".into(),
            ));
        }
        let (token, id, hash) = new_opaque(kind);
        let now = now_ms();
        let rec = TokenRecord {
            id,
            hash,
            kind: kind.to_owned(),
            subject: subject.to_owned(),
            groups,
            description: description.to_owned(),
            created_at_ms: now,
            expires_at_ms: ttl.map(|t| now.saturating_add(i64::try_from(t.as_millis()).unwrap_or(i64::MAX))),
        };
        self.meta()?.put_token(&rec).await?;
        Ok((token, rec))
    }

    pub async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>, AuthError> {
        Ok(self.meta()?.list_tokens(subject).await?)
    }

    pub async fn revoke_token(&self, id: &str) -> Result<bool, AuthError> {
        Ok(self.meta()?.delete_token(id).await?)
    }

    pub async fn start_device_login(&self) -> Result<DeviceStart, AuthError> {
        match (&self.oidc, &self.github, &self.issuer) {
            (Some(o), _, Some(_)) => Ok(o.start().await?),
            (None, Some(g), Some(_)) => Ok(g.start().await?),
            _ => Err(AuthError::Unavailable(
                "device login is not configured on this server".into(),
            )),
        }
    }

    /// 轮询 device login；完成时返回 (刷新令牌, 访问令牌, 过期时间, 身份)。
    pub async fn poll_device_login(
        &self,
        device_code: &str,
    ) -> Result<(DevicePoll, Option<(String, String, i64)>), AuthError> {
        let poll = match (&self.oidc, &self.github, &self.issuer) {
            (Some(o), _, Some(_)) => o.poll(device_code).await?,
            (None, Some(g), Some(_)) => g.poll(device_code).await?,
            _ => {
                return Err(AuthError::Unavailable(
                    "device login is not configured on this server".into(),
                ));
            }
        };
        let DevicePoll::Complete(p) = &poll else {
            return Ok((poll, None));
        };
        let (refresh, refresh_rec) = self
            .create_token("refresh", &p.subject, p.groups.clone(), "scn login", Some(REFRESH_TTL))
            .await?;
        let (access, exp) = self.issue(p, Some((&refresh_rec.id, refresh_rec.expires_at_ms)))?;
        Ok((poll, Some((refresh, access, exp))))
    }

    // ---------- 策略 ----------

    /// 校验并保存策略（本节点立即生效），返回规则数。空文本是一份没有规则的策略：拒绝一切。
    pub async fn set_policy(&self, text: &str) -> Result<usize, AuthError> {
        let p = Policy::parse(text).map_err(|e| AuthError::BadRequest(e.to_string()))?;
        self.meta()?.put_setting(POLICY_KEY, text.as_bytes()).await?;
        let n = p.len();
        self.install(PolicyState::Set(Arc::new(p)));
        Ok(n)
    }

    pub async fn policy_text(&self) -> Result<String, AuthError> {
        let v = self.meta()?.get_setting(POLICY_KEY).await?;
        Ok(v.map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default())
    }

    /// 从元数据库重新加载策略（启动时和定期调用，让其它节点的修改生效）。失败时保留原来的策略；
    /// 从未加载成功时保持拒绝一切。
    pub async fn reload_policy(&self) -> Result<usize, AuthError> {
        let Some(raw) = self.meta()?.get_setting(POLICY_KEY).await? else {
            if self.require_policy {
                self.install(PolicyState::Unloaded);
                return Err(AuthError::Unavailable(
                    "an explicit access policy is required before serving".into(),
                ));
            }
            self.install(PolicyState::Unset);
            return Ok(0);
        };
        let text = String::from_utf8(raw).map_err(|e| AuthError::Unavailable(format!("stored policy: {e}")))?;
        let p = Policy::parse(&text).map_err(|e| AuthError::Unavailable(format!("stored policy: {e}")))?;
        let n = p.len();
        self.install(PolicyState::Set(Arc::new(p)));
        Ok(n)
    }

    /// 策略是否已加载（不要求认证时总是 true）。
    pub fn policy_loaded(&self) -> bool {
        !self.required || !matches!(self.state(), PolicyState::Unloaded)
    }

    fn install(&self, p: PolicyState) {
        if let Ok(mut cur) = self.policy.write() {
            *cur = p;
        }
    }

    fn state(&self) -> PolicyState {
        self.policy.read().map_or(PolicyState::Unloaded, |p| p.clone())
    }

    /// 当前生效的策略；还没加载成功时返回 Unavailable。
    fn current_policy(&self) -> Result<Option<Arc<Policy>>, AuthError> {
        match self.state() {
            PolicyState::Unloaded => Err(AuthError::Unavailable(
                "the access policy has not been loaded yet".into(),
            )),
            PolicyState::Unset => Ok(None),
            PolicyState::Set(p) => Ok(Some(p)),
        }
    }

    // ---------- 鉴权 ----------

    /// A webhook's answer: abstains for repositories it does not cover, and when unavailable
    /// with `on_error = "open"`.
    async fn webhook_decides(&self, w: &AuthzWebhook, p: &Principal, req: &Request<'_>) -> Result<Decision, AuthError> {
        if !w.covers(req.repo) {
            return Ok(Decision::Abstain);
        }
        match w.allows(p, req).await {
            Ok(true) => Ok(Decision::Allow),
            Ok(false) if w.denies() => Ok(Decision::Deny),
            Ok(false) => Ok(Decision::Abstain),
            Err(e) if w.fail_open() => {
                tracing::warn!(webhook = w.name(), error = %e, "authz webhook unavailable; asking the next decider");
                Ok(Decision::Abstain)
            }
            Err(e) => Err(AuthError::Unavailable(e)),
        }
    }

    /// One decider's answer. `admin` asks for system administration (policies, tokens) rather
    /// than `req` itself.
    async fn decide(&self, d: Decider, p: &Principal, req: &Request<'_>, admin: bool) -> Result<Decision, AuthError> {
        let yes = |b: bool| if b { Decision::Allow } else { Decision::Abstain };
        Ok(match d {
            Decider::Policy => match self.current_policy()? {
                Some(pol) if admin => yes(pol.is_system_admin(p)),
                Some(pol) => match pol.decide(p, req) {
                    Some(true) => Decision::Allow,
                    Some(false) => Decision::Deny,
                    None => Decision::Abstain,
                },
                // The built-in default settles the request either way
                None if self.webhooks.is_empty() => {
                    let builtin = Policy::builtin();
                    let allowed = if admin {
                        builtin.is_system_admin(p)
                    } else {
                        builtin.allows(p, req)
                    };
                    if allowed { Decision::Allow } else { Decision::Deny }
                }
                None => Decision::Abstain,
            },
            Decider::Grants => yes(!admin && grants_allow(p, req)),
            Decider::Webhook(i) => self.webhook_decides(&self.webhooks[i], p, req).await?,
            Decider::Orgs => match (&self.orgs, crate::orgs::org_of(req.repo)) {
                (Some(orgs), Some(org)) if !admin => {
                    let visibility = self.org_visibility(orgs, org).await?;
                    yes(crate::orgs::orgs_allow(p, req, visibility.as_deref()))
                }
                _ => Decision::Abstain,
            },
            Decider::Personal => yes(!admin && crate::orgs::personal_allow(p, req.repo)),
        })
    }

    /// `p` with its organization memberships (`org:ORG:ROLE`) and then the policy's `[groups]`
    /// added: every decider sees them.
    async fn expand<'p>(&self, p: &'p Principal) -> Result<std::borrow::Cow<'p, Principal>, AuthError> {
        let mut p = std::borrow::Cow::Borrowed(p);
        if let (Some(orgs), true) = (&self.orgs, p.authenticated) {
            let groups = self.memberships(orgs, &p.subject).await?;
            if !groups.is_empty() {
                p.to_mut().groups.extend(groups.iter().cloned());
            }
        }
        if let (Some(teams), true) = (&self.teams, p.authenticated) {
            let groups = teams.groups(&p.subject).await;
            if !groups.is_empty() {
                p.to_mut().groups.extend(groups);
            }
        }
        // No copy on the hot path when there are no groups to add
        Ok(match self.current_policy()? {
            Some(pol) if pol.has_groups() => std::borrow::Cow::Owned(pol.with_groups(&p)),
            _ => p,
        })
    }

    async fn memberships(&self, orgs: &OrgCache, subject: &str) -> Result<Arc<Vec<String>>, AuthError> {
        if let Some(hit) = orgs.memberships.get(subject).await {
            return Ok(hit);
        }
        let groups: Vec<String> = self
            .meta()?
            .list_memberships(subject)
            .await?
            .into_iter()
            .filter_map(|m| crate::orgs::Role::parse(&m.role).map(|r| crate::orgs::membership_group(&m.org, r)))
            .collect();
        let groups = Arc::new(groups);
        orgs.memberships.insert(subject.to_owned(), groups.clone()).await;
        Ok(groups)
    }

    async fn org_visibility(&self, orgs: &OrgCache, org: &str) -> Result<Option<String>, AuthError> {
        if let Some(hit) = orgs.visibility.get(org).await {
            return Ok(hit);
        }
        let v = self.meta()?.get_org(org).await?.map(|o| o.visibility);
        orgs.visibility.insert(org.to_owned(), v.clone()).await;
        Ok(v)
    }

    /// Drops cached memberships and visibility after a change through this node, so it takes
    /// effect at once here (other nodes within [`ORG_CACHE_TTL`]).
    pub async fn forget_org_changes(&self, org: Option<&str>, subject: Option<&str>) {
        if let Some(orgs) = &self.orgs {
            if let Some(o) = org {
                orgs.visibility.invalidate(o).await;
            }
            match subject {
                Some(s) => orgs.memberships.invalidate(s).await,
                None => orgs.memberships.invalidate_all(),
            }
        }
    }

    /// Runs the chain: the first decider that allows or denies settles it; all abstaining denies.
    async fn run_chain(&self, p: &Principal, req: &Request<'_>, admin: bool) -> Result<bool, AuthError> {
        let p = &*self.expand(p).await?;
        for d in &self.chain {
            match self.decide(*d, p, req, admin).await? {
                Decision::Allow => return Ok(true),
                Decision::Deny => return Ok(false),
                Decision::Abstain => {}
            }
        }
        Ok(false)
    }

    /// 是否允许。
    pub async fn check(&self, p: &Principal, req: &Request<'_>) -> Result<bool, AuthError> {
        if !self.required {
            return Ok(true);
        }
        self.run_chain(p, req, false).await
    }

    /// 在仓库里是否有 `action`（不论 ref 和路径的限制），见 [`Policy::allows_any`]。
    pub async fn check_any(&self, p: &Principal, repo: &str, action: Action) -> Result<bool, AuthError> {
        if !self.required {
            return Ok(true);
        }
        let expanded = self.expand(p).await?;
        if self
            .current_policy()?
            .is_some_and(|pol| pol.allows_any(&expanded, repo, action))
        {
            return Ok(true);
        }
        self.check(p, &Request::repo(repo, action)).await
    }

    /// 同 [`Guard::check_any`]，不允许时返回错误。
    pub async fn authorize_any(&self, p: &Principal, repo: &str, action: Action) -> Result<(), AuthError> {
        if self.check_any(p, repo, action).await? {
            return Ok(());
        }
        self.authorize(p, &Request::repo(repo, action)).await
    }

    /// 不允许时返回 [`AuthError::Denied`]（未认证的请求返回 [`AuthError::Missing`]）。
    pub async fn authorize(&self, p: &Principal, req: &Request<'_>) -> Result<(), AuthError> {
        if self.check(p, req).await? {
            return Ok(());
        }
        if !p.authenticated {
            return Err(AuthError::Missing);
        }
        Err(AuthError::Denied {
            repo: req.repo.to_owned(),
            action: req.action.as_str(),
            reference: req.reference.map(str::to_owned),
            path: req.path.map(str::to_owned),
        })
    }

    /// 列目录时条目 `path` 能否显示（可读，或是某个可读路径的上级目录）。
    pub async fn path_visible(&self, p: &Principal, repo: &str, path: &str) -> Result<bool, AuthError> {
        if self
            .check(p, &Request::repo(repo, Action::Read).with_path(path))
            .await?
        {
            return Ok(true);
        }
        let expanded = self.expand(p).await?;
        Ok(self
            .current_policy()?
            .is_some_and(|pol| pol.path_visible(&expanded, repo, path)))
    }

    /// 是否有系统管理权限（策略、令牌）。
    pub async fn is_admin(&self, p: &Principal) -> Result<bool, AuthError> {
        if !self.required {
            return Ok(true);
        }
        self.run_chain(p, &Request::repo("*", Action::Admin), true).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientAuth, IssuerConfig, IssuerKey, OidcConfig, Secret, WebhookConfig};
    use crate::testing::FakeIdp;
    use scion_store::MemStore;
    use serde_json::json;

    fn lookup() -> Lookup {
        Arc::new(|k: &str| (k == "STARHUB_JWT_SIGNING_KEY").then(|| "signing-key".to_string()))
    }

    #[tokio::test]
    async fn the_chain_runs_in_order_and_webhooks_cover_their_repos() {
        let idp = FakeIdp::start().await;
        let text = format!(
            r#"
[authorization]
chain = ["webhook:starhub", "policy", "webhook:opa"]

[[authorization.webhook]]
name = "starhub"
url = "{authz}"
bearer = "opa"
decides = "allow-deny"
repos = ["models/*"]

[[authorization.webhook]]
name = "opa"
url = "{authz}"
bearer = "opa"
"#,
            authz = idp.url("/authz")
        );
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::parse(&text).expect("config"),
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("build");
        g.set_policy("[[rule]]\nsubjects = [\"*\"]\nrepos = [\"*\"]\nactions = [\"write\"]")
            .await
            .expect("policy");
        let alice = Principal::user("alice", vec![]);
        let (g, alice) = (&g, &alice);
        let ok = |repo: &'static str, action| async move {
            g.check(alice, &Request::repo(repo, action)).await.expect("check")
        };
        // starhub settles models/*, and its false denies although the policy allows writing
        assert!(ok("models/a", Action::Read).await);
        assert!(!ok("models/a", Action::Write).await);
        // Elsewhere starhub abstains and the policy decides
        assert!(ok("internal/x", Action::Write).await);
        // Nobody allows landing: opa's false only abstains, and all abstaining denies
        assert!(!ok("internal/x", Action::Land).await);
        assert!(
            g.check(&Principal::anonymous(), &Request::repo("public", Action::Read))
                .await
                .expect("check"),
            "opa, last in the chain"
        );
        assert!(!g.is_admin(&Principal::user("alice", vec![])).await.expect("admin"));
    }

    #[tokio::test]
    async fn github_device_login_issues_scion_tokens() {
        let idp = FakeIdp::start().await;
        let text = format!(
            "[github]\nclient_id = \"gh-app\"\nweb_url = \"{gh}\"\napi_url = \"{gh}\"",
            gh = idp.url("/gh")
        );
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::parse(&text).expect("config"),
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("build");
        assert!(g.device_login());
        let start = g.start_device_login().await.expect("start");
        assert_eq!(start.user_code, "WXYZ-1234");
        let (poll, tokens) = g.poll_device_login(&start.device_code).await.expect("poll");
        assert_eq!((poll, tokens), (DevicePoll::Pending, None));
        idp.approve(&start.device_code);
        let (_, tokens) = g.poll_device_login(&start.device_code).await.expect("poll");
        let (refresh, access, _) = tokens.expect("tokens");
        assert!(refresh.starts_with("scn_refresh_"));
        let p = g.authenticate(Some(&access)).await.expect("access token");
        assert_eq!(p.subject, "octocat");
        // Without a signing key there is no device login
        let no_key = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::parse(&text).expect("config"),
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("build");
        assert!(!no_key.device_login());
        assert!(matches!(
            no_key.start_device_login().await,
            Err(AuthError::Unavailable(_))
        ));
        assert!(matches!(
            no_key.poll_device_login("x").await,
            Err(AuthError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn github_teams_reach_the_policy() {
        let idp = FakeIdp::start().await;
        let text = format!(
            "[github]\nclient_id = \"gh-app\"\napi_url = \"{gh}\"\norgs = [\"opencsg\"]\ntoken_env = \"GITHUB_TOKEN\"",
            gh = idp.url("/gh")
        );
        let config = GuardConfig {
            required: true,
            auth: AuthConfig::parse(&text).expect("config"),
            signing_key_pem: None,
        };
        assert!(
            Guard::build(config.clone(), Arc::new(MemStore::new()), lookup()).is_err(),
            "GITHUB_TOKEN unset"
        );
        let g = Guard::build(
            config,
            Arc::new(MemStore::new()),
            Arc::new(|k: &str| (k == "GITHUB_TOKEN").then(|| "ght".to_string())),
        )
        .expect("build");
        g.set_policy("[[rule]]\nsubjects = [\"group:github:opencsg/core\"]\nrepos = [\"mono\"]\nactions = [\"land\"]")
            .await
            .expect("policy");
        let land = Request::repo("mono", Action::Land);
        assert!(
            g.check(&Principal::user("octocat", vec![]), &land)
                .await
                .expect("check")
        );
        assert!(
            !g.check(&Principal::user("alice", vec![]), &land).await.expect("check"),
            "not in core"
        );
        assert!(!g.check(&Principal::anonymous(), &land).await.expect("check"));
    }

    #[tokio::test]
    async fn organizations_and_personal_namespaces() {
        use scion_store::{OrgMember, OrgRecord};
        let meta = Arc::new(MemStore::new());
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::parse("[authorization]\nchain = [\"policy\", \"orgs\", \"personal\"]")
                    .expect("config"),
                signing_key_pem: None,
            },
            meta.clone(),
            lookup(),
        )
        .expect("build");
        g.set_policy("[[rule]]\nsubjects = [\"group:org:acme:owner\"]\nrepos = [\"shared\"]\nactions = [\"land\"]")
            .await
            .expect("policy");
        let org = |name: &str, visibility: &str| OrgRecord {
            name: name.into(),
            visibility: visibility.into(),
            created_at_ms: 0,
            created_by: "root".into(),
        };
        let member = |org: &str, subject: &str, role: &str| OrgMember {
            org: org.into(),
            subject: subject.into(),
            role: role.into(),
            added_at_ms: 0,
            added_by: "root".into(),
        };
        meta.create_org(&org("acme", "private")).await.expect("org");
        meta.create_org(&org("open", "internal")).await.expect("org");
        meta.put_org_member(&member("acme", "alice", "owner"))
            .await
            .expect("member");
        meta.put_org_member(&member("acme", "bob", "developer"))
            .await
            .expect("member");
        meta.put_org_member(&member("acme", "carl", "nonsense"))
            .await
            .expect("member");
        let (alice, bob, eve) = (
            Principal::user("alice", vec![]),
            Principal::user("bob", vec![]),
            Principal::user("eve", vec![]),
        );
        let can = |p: &Principal, repo: &str, action| {
            let (p, repo) = (p.clone(), repo.to_owned());
            let g = g.clone();
            async move { g.check(&p, &Request::repo(&repo, action)).await.expect("check") }
        };
        assert!(
            can(&alice, "acme/new", Action::Admin).await,
            "an owner creates repositories"
        );
        assert!(can(&bob, "acme/app", Action::Write).await);
        assert!(!can(&bob, "acme/app", Action::Land).await);
        assert!(!can(&eve, "acme/app", Action::Read).await, "private");
        assert!(can(&eve, "open/app", Action::Read).await && !can(&eve, "open/app", Action::Write).await);
        assert!(
            !can(&Principal::user("carl", vec![]), "acme/app", Action::Read).await,
            "unknown role"
        );
        assert!(
            can(&alice, "shared", Action::Land).await,
            "policies can name membership groups"
        );
        assert!(g.check_any(&alice, "shared", Action::Land).await.expect("any"));
        assert!(
            !g.is_admin(&alice).await.expect("admin"),
            "an owner is not a system administrator"
        );
        assert!(can(&alice, "user/alice/notes", Action::Admin).await);
        assert!(!can(&alice, "user/bob/notes", Action::Read).await);
        // Changes through this node take effect at once once forgotten
        meta.put_org_member(&member("acme", "bob", "maintainer"))
            .await
            .expect("role");
        assert!(!can(&bob, "acme/app", Action::Land).await, "still cached");
        g.forget_org_changes(Some("acme"), Some("bob")).await;
        assert!(can(&bob, "acme/app", Action::Land).await);
        meta.set_org_visibility("acme", "internal").await.expect("visibility");
        g.forget_org_changes(Some("acme"), None).await;
        assert!(can(&eve, "acme/app", Action::Read).await);
        assert!(can(&Principal::user("x", vec![]), "acme/app", Action::Read).await);
        assert!(!can(&Principal::anonymous(), "acme/app", Action::Read).await);
        // Without orgs in the chain, memberships are not looked up
        Guard::disabled().forget_org_changes(Some("acme"), None).await;
    }

    #[tokio::test]
    async fn policy_denials_and_groups_reach_the_chain() {
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::default(),
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("build");
        g.set_policy(
            r#"
[groups]
eng = ["user:alice"]
ops = ["group:eng"]

[[rule]]
subjects = ["group:ops"]
repos = ["mono"]
actions = ["land"]

[[rule]]
effect = "deny"
subjects = ["group:eng"]
repos = ["mono"]
paths = ["secrets"]
actions = ["read"]

[[rule]]
subjects = ["group:ops"]
repos = ["*"]
actions = ["admin"]
"#,
        )
        .await
        .expect("policy");
        let alice = Principal::user("alice", vec![]);
        let read = |path| Request::repo("mono", Action::Read).with_path(path);
        assert!(
            g.check(&alice, &read("src")).await.expect("check"),
            "group from [groups]"
        );
        assert!(!g.check(&alice, &read("secrets/k")).await.expect("check"), "denied");
        assert!(g.check_any(&alice, "mono", Action::Land).await.expect("any"));
        assert!(g.path_visible(&alice, "mono", "src").await.expect("visible"));
        assert!(g.is_admin(&alice).await.expect("admin"));
        assert!(
            !g.check(&Principal::user("bob", vec![]), &read("src"))
                .await
                .expect("check")
        );
    }

    async fn guard(idp: &FakeIdp, webhook: bool) -> Arc<Guard> {
        let mut auth = AuthConfig::default();
        auth.token.issuer = "https://scion.test".into();
        auth.issuers = vec![
            IssuerConfig {
                issuer: "OpenCSG".into(),
                key: IssuerKey::Hs256(Secret {
                    env: Some("STARHUB_JWT_SIGNING_KEY".into()),
                    ..Secret::default()
                }),
                audience: None,
                subject_claim: "current_user".into(),
                groups_claim: None,
                repos_claim: None,
            },
            IssuerConfig {
                issuer: idp.base(),
                key: IssuerKey::Jwks(idp.url("/jwks")),
                audience: Some("scion".into()),
                subject_claim: "sub".into(),
                groups_claim: Some("groups".into()),
                repos_claim: Some("repos".into()),
            },
        ];
        auth.oidc = Some(OidcConfig {
            issuer: idp.base(),
            client_id: "scn".into(),
            client_secret_env: None,
            scopes: vec!["openid".into()],
            subject_claim: "preferred_username".into(),
            groups_claim: Some("groups".into()),
        });
        if webhook {
            auth.authz_webhook = Some(WebhookConfig {
                url: idp.url("/authz"),
                auth: ClientAuth::Bearer(Secret {
                    value: Some("opa".into()),
                    ..Secret::default()
                }),
                cache_ttl: Duration::from_secs(30),
                timeout: Duration::from_secs(2),
                fail_open: false,
                ..WebhookConfig::default()
            });
        }
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth,
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        g.reload_policy().await.expect("load policy");
        g
    }

    #[tokio::test]
    async fn required_policy_fails_closed_until_explicitly_initialized() {
        let config = AuthConfig::parse("[authorization]\nrequire_policy = true").expect("config");
        let store = Arc::new(MemStore::new());
        assert!(
            Guard::build(
                GuardConfig {
                    auth: config.clone(),
                    ..GuardConfig::default()
                },
                store.clone(),
                lookup()
            )
            .is_err(),
            "must not silently disable authentication"
        );
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: config,
                signing_key_pem: None,
            },
            store,
            lookup(),
        )
        .expect("guard");
        assert!(
            g.reload_policy().await.is_err(),
            "absent policy must not enable defaults"
        );
        assert!(!g.policy_loaded());
        let alice = Principal::user("alice", vec![]);
        assert!(matches!(
            g.check(&alice, &Request::repo("other/repo", Action::Read)).await,
            Err(AuthError::Unavailable(_))
        ));
        g.set_policy("").await.expect("explicit deny-all policy");
        assert_eq!(g.reload_policy().await.expect("load"), 0);
        assert!(g.policy_loaded());
        assert!(
            !g.check(&alice, &Request::repo("other/repo", Action::Read))
                .await
                .expect("deny")
        );
    }

    #[test]
    fn build_without_calls_out_needs_no_http_client() {
        // Nothing to call: no client is built, so the server starts even without CA certificates
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::default(),
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        assert!(g.externals.is_empty() && g.introspection.is_none() && g.webhooks.is_empty() && g.oidc.is_none());
        assert!(g.required() && g.can_issue() && !g.device_login());
        http_client().expect("an HTTP client where CA certificates exist");
    }

    fn now() -> i64 {
        now_ms() / 1000
    }

    fn starhub_jwt(user: &str, exp: i64) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({"uuid": "u", "current_user": user, "iss": "OpenCSG", "exp": exp}),
            &EncodingKey::from_secret(b"signing-key"),
        )
        .expect("sign")
    }

    #[tokio::test]
    async fn exchanged_jwt_is_bound_to_its_source_record() {
        let meta: Arc<dyn MetaStore> = Arc::new(MemStore::new());
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::default(),
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            meta,
            lookup(),
        )
        .expect("guard");
        let (pat, rec) = g.create_token("pat", "alice", vec![], "", None).await.expect("pat");
        let (jwt, _, _) = g.exchange(&pat).await.expect("exchange");
        assert_eq!(g.authenticate(Some(&jwt)).await.expect("bound").subject, "alice");
        // A JWT for another subject that names alice's record is refused.
        let (forged, _) = g
            .issue(&Principal::user("mallory", vec![]), Some((&rec.id, None)))
            .expect("issue");
        assert!(matches!(
            g.authenticate(Some(&forged)).await,
            Err(AuthError::Invalid(_))
        ));
        // A JWT without a source stays valid (external exchange, or issued before the upgrade).
        let (plain, _) = g.issue(&Principal::user("bob", vec![]), None).expect("issue");
        assert_eq!(g.authenticate(Some(&plain)).await.expect("plain").subject, "bob");
        assert!(g.revoke_token(&rec.id).await.expect("revoke"));
        assert!(matches!(g.authenticate(Some(&jwt)).await, Err(AuthError::Invalid(_))));
    }

    #[tokio::test]
    async fn deny_all_guard_rejects_everyone() {
        let g = Guard::deny_all();
        assert!(g.required());
        let anon = g.authenticate(None).await.expect("anonymous");
        assert!(!g.check(&anon, &Request::repo("x", Action::Read)).await.expect("ok"));
        assert!(g.authenticate(Some("scn_pat_x")).await.is_err());
        assert!(!g.is_admin(&anon).await.expect("ok"));
    }

    #[tokio::test]
    async fn unloaded_policy_denies_everything() {
        let meta: Arc<dyn MetaStore> = Arc::new(MemStore::new());
        meta.put_setting(POLICY_KEY, b"broken [").await.expect("set");
        let g = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::default(),
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            meta.clone(),
            lookup(),
        )
        .expect("guard");
        assert!(!g.policy_loaded());
        // 自签令牌不查库也能验证，但策略没加载成功时不能退回默认放行
        let (token, _) = g.issue(&Principal::user("alice", vec![]), None).expect("issue");
        let alice = g.authenticate(Some(&token)).await.expect("jwt");
        let unavailable = |r: Result<bool, AuthError>| matches!(r, Err(AuthError::Unavailable(_)));
        assert!(unavailable(g.check(&alice, &Request::repo("x", Action::Read)).await));
        assert!(unavailable(g.check_any(&alice, "x", Action::Read).await));
        assert!(unavailable(g.path_visible(&alice, "x", "a").await));
        assert!(unavailable(g.is_admin(&alice).await));
        assert!(g.reload_policy().await.is_err(), "broken stored policy");
        assert!(!g.policy_loaded(), "a failed reload keeps denying");
        meta.put_setting(POLICY_KEY, &[0xff]).await.expect("set");
        assert!(g.reload_policy().await.is_err(), "not UTF-8");
        // 修好后加载成功
        meta.put_setting(
            POLICY_KEY,
            b"[[rule]]\nsubjects=[\"*\"]\nrepos=[\"x\"]\nactions=[\"read\"]",
        )
        .await
        .expect("fix");
        assert_eq!(g.reload_policy().await.expect("reload"), 1);
        assert!(g.policy_loaded());
        assert!(g.check(&alice, &Request::repo("x", Action::Read)).await.expect("ok"));
        assert!(!g.check(&alice, &Request::repo("y", Action::Read)).await.expect("ok"));
        // 库里没有策略：默认放行
        let fresh = Guard::build(
            GuardConfig {
                required: true,
                auth: AuthConfig::default(),
                signing_key_pem: Some(crate::token::generate_signing_key()),
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        assert_eq!(fresh.reload_policy().await.expect("reload"), 0);
        assert!(
            fresh
                .check(&alice, &Request::repo("y", Action::Land))
                .await
                .expect("ok")
        );
        assert!(
            !fresh
                .check(&alice, &Request::repo("y", Action::Admin))
                .await
                .expect("ok")
        );
        assert!(!fresh.is_admin(&alice).await.expect("ok"));
        // 默认策略：admins 组可以管理
        let (token, _) = fresh
            .issue(&Principal::user("root", vec!["admins".into()]), None)
            .expect("issue");
        let root = fresh.authenticate(Some(&token)).await.expect("jwt");
        assert!(fresh.is_admin(&root).await.expect("ok"));
        assert!(
            fresh
                .check(&root, &Request::repo("y", Action::Admin))
                .await
                .expect("ok")
        );
        assert!(Guard::disabled().policy_loaded());
    }

    #[tokio::test]
    async fn disabled_guard_allows_everything() {
        let g = Guard::disabled();
        assert!(!g.required() && !g.can_issue() && !g.device_login());
        let p = g.authenticate(Some("whatever")).await.expect("anonymous");
        assert!(!p.authenticated);
        assert!(g.check(&p, &Request::repo("x", Action::Admin)).await.expect("ok"));
        assert!(g.is_admin(&p).await.expect("ok"));
        assert!(g.create_token("pat", "a", vec![], "", None).await.is_err(), "no store");
        assert!(g.exchange("scn_pat_x").await.is_err());
        assert!(g.start_device_login().await.is_err());
        assert!(g.poll_device_login("x").await.is_err());
    }

    #[tokio::test]
    async fn authenticates_every_token_kind() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false).await;
        assert!(g.required() && g.can_issue() && g.device_login());
        assert!(!g.authenticate(None).await.expect("anon").authenticated);
        assert!(!g.authenticate(Some("  ")).await.expect("anon").authenticated);

        // PAT
        let (pat, rec) = g
            .create_token("pat", "ci-bot", vec!["ci".into()], "CI", Some(Duration::from_secs(60)))
            .await
            .expect("pat");
        let p = g.authenticate(Some(&pat)).await.expect("pat");
        assert_eq!(
            (p.subject.as_str(), p.groups.as_slice()),
            ("ci-bot", &["ci".to_string()][..])
        );
        assert_eq!(
            g.list_tokens(Some("ci-bot")).await.expect("list"),
            std::slice::from_ref(&rec)
        );
        assert!(g.revoke_token(&rec.id).await.expect("revoke"));
        assert!(matches!(g.authenticate(Some(&pat)).await, Err(AuthError::Invalid(_))));
        let (old, _) = g
            .create_token("pat", "x", vec![], "", Some(Duration::ZERO))
            .await
            .expect("pat");
        assert_eq!(g.authenticate(Some(&old)).await, Err(AuthError::Expired));
        let (refresh, _) = g.create_token("refresh", "x", vec![], "", None).await.expect("refresh");
        assert!(
            g.authenticate(Some(&refresh)).await.is_err(),
            "refresh tokens are not access tokens"
        );
        assert!(g.create_token("pat", " ", vec![], "", None).await.is_err());
        assert!(g.create_token("pat", "a", vec!["a\nb".into()], "", None).await.is_err());

        // starhub 风格的 HS256 JWT
        let p = g
            .authenticate(Some(&starhub_jwt("alice", now() + 60)))
            .await
            .expect("starhub");
        assert_eq!(p.subject, "alice");
        assert_eq!(
            g.authenticate(Some(&starhub_jwt("alice", now() - 3600))).await,
            Err(AuthError::Expired)
        );

        // JWKS 签发方，带 repos claim
        let t = idp.sign(
            &json!({"iss": idp.base(), "aud": "scion", "sub": "svc", "groups": ["g"],
            "repos": {"models/*": "write"}, "exp": now() + 60}),
        );
        let p = g.authenticate(Some(&t)).await.expect("jwks");
        assert_eq!((p.subject.as_str(), p.grants.len()), ("svc", 1));
        let wrong_aud = idp.sign(&json!({"iss": idp.base(), "aud": "other", "sub": "svc", "exp": now() + 60}));
        assert!(g.authenticate(Some(&wrong_aud)).await.is_err());

        // 自签令牌：exchange 得到，可以直接用，但不能再拿去 exchange
        let (access, exp, subject) = g.exchange(&starhub_jwt("alice", now() + 60)).await.expect("exchange");
        assert_eq!(subject, "alice");
        assert!(exp > now_ms());
        let p = g.authenticate(Some(&access)).await.expect("own jwt");
        assert_eq!((p.subject.as_str(), p.expires_at_ms), ("alice", Some(exp)));
        assert!(g.exchange(&access).await.is_err());
        assert_eq!(g.exchange(&refresh).await.expect("refresh exchange").2, "x");
        assert!(matches!(g.exchange(" ").await, Err(AuthError::BadRequest(_))));

        // 未知签发方、无法识别的令牌（没有配置内省）
        let unknown = starhub_jwt("a", now() + 60).replace("eyJ", "eyK");
        assert!(g.authenticate(Some(&unknown)).await.is_err());
        assert!(matches!(
            g.authenticate(Some("opaque-foreign")).await,
            Err(AuthError::Invalid(_))
        ));
        assert!(g.exchange("opaque-foreign").await.is_err());
    }

    #[tokio::test]
    async fn device_login_creates_refresh_and_access_tokens() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false).await;
        let s = g.start_device_login().await.expect("start");
        let (state, tokens) = g.poll_device_login(&s.device_code).await.expect("poll");
        assert_eq!((state, tokens), (DevicePoll::Pending, None));
        idp.approve(&s.device_code);
        let (state, tokens) = g.poll_device_login(&s.device_code).await.expect("poll");
        assert!(matches!(state, DevicePoll::Complete(_)));
        let (refresh, access, _) = tokens.expect("tokens");
        assert_eq!(g.authenticate(Some(&access)).await.expect("access").subject, "alice");
        assert_eq!(g.exchange(&refresh).await.expect("refresh").2, "alice");
        let listed = g.list_tokens(Some("alice")).await.expect("list");
        assert_eq!((listed.len(), listed[0].kind.as_str()), (1, "refresh"));
        // The access token is bound to the new refresh record.
        assert!(g.revoke_token(&listed[0].id).await.expect("revoke"));
        assert!(g.authenticate(Some(&access)).await.is_err());
    }

    #[tokio::test]
    async fn authorization_combines_policy_grants_and_webhook() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, false).await;
        let alice = Principal::user("alice", vec!["eng".into()]);
        let anon = Principal::anonymous();
        // 没有策略和回调：已认证的身份可以读写合入，不能管理；匿名不行
        assert!(g.check(&alice, &Request::repo("any", Action::Land)).await.expect("ok"));
        assert!(!g.is_admin(&alice).await.expect("ok"));
        assert_eq!(
            g.authorize(&anon, &Request::repo("any", Action::Read)).await,
            Err(AuthError::Missing)
        );

        assert_eq!(g.policy_text().await.expect("text"), "");
        let n = g
            .set_policy(
                "[[rule]]\nsubjects=[\"group:eng\"]\nrepos=[\"mono\"]\nactions=[\"write\"]\npaths=[\"src\"]\n\n\
                 [[rule]]\nsubjects=[\"user:root\"]\nrepos=[\"*\"]\nactions=[\"admin\"]\n",
            )
            .await
            .expect("set");
        assert_eq!(n, 2);
        assert!(
            g.check(&alice, &Request::repo("mono", Action::Write).with_path("src/a"))
                .await
                .expect("ok")
        );
        let denied = g.authorize(&alice, &Request::repo("mono", Action::Read)).await;
        assert!(
            matches!(denied, Err(AuthError::Denied { action: "read", .. })),
            "{denied:?}"
        );
        assert!(g.path_visible(&alice, "mono", "src").await.expect("ok"));
        assert!(g.check_any(&alice, "mono", Action::Write).await.expect("ok"));
        g.authorize_any(&alice, "mono", Action::Read).await.expect("any read");
        assert!(matches!(
            g.authorize_any(&alice, "mono", Action::Land).await,
            Err(AuthError::Denied { action: "land", .. })
        ));
        assert!(!g.path_visible(&alice, "mono", "secret").await.expect("ok"));
        assert!(g.is_admin(&Principal::user("root", vec![])).await.expect("ok"));
        assert!(matches!(g.set_policy("[[rule]]").await, Err(AuthError::BadRequest(_))));

        // 令牌自带的授权
        let mut svc = Principal::user("svc", vec![]);
        svc.grants = vec![crate::policy::Grant {
            repo: "models/*".into(),
            action: Action::Write,
        }];
        assert!(
            g.check(&svc, &Request::repo("models/x", Action::Write))
                .await
                .expect("ok")
        );

        // 显式保存的空策略拒绝一切，重新加载后（其它节点）仍然拒绝，不退回默认放行
        assert_eq!(g.set_policy("  \n").await.expect("empty"), 0);
        assert!(
            !g.check(&alice, &Request::repo("other", Action::Read))
                .await
                .expect("ok")
        );
        assert_eq!(g.reload_policy().await.expect("reload"), 0);
        assert!(
            !g.check(&alice, &Request::repo("other", Action::Read))
                .await
                .expect("still denied")
        );
        // 其它节点修改了策略：重新加载
        g.meta()
            .expect("meta")
            .put_setting(
                POLICY_KEY,
                b"[[rule]]\nsubjects=[\"*\"]\nrepos=[\"r\"]\nactions=[\"read\"]",
            )
            .await
            .expect("set");
        assert_eq!(g.reload_policy().await.expect("reload"), 1);
        g.meta()
            .expect("meta")
            .put_setting(POLICY_KEY, b"broken [")
            .await
            .expect("set");
        assert!(
            g.reload_policy().await.is_err(),
            "a broken stored policy keeps the old one"
        );
        assert!(
            g.check(&alice, &Request::repo("r", Action::Read))
                .await
                .expect("old policy")
        );
    }

    #[tokio::test]
    async fn webhook_decides_when_configured() {
        let idp = FakeIdp::start().await;
        let g = guard(&idp, true).await;
        let alice = Principal::user("alice", vec![]);
        // 有回调时没有「已认证即可」的默认
        assert!(
            g.check(&alice, &Request::repo("models/a", Action::Read))
                .await
                .expect("ok")
        );
        assert!(
            !g.check(&alice, &Request::repo("models/a", Action::Write))
                .await
                .expect("ok")
        );
        assert!(
            g.check(&Principal::anonymous(), &Request::repo("public", Action::Read))
                .await
                .expect("public repo")
        );
        assert!(!g.is_admin(&alice).await.expect("ok"));
        // 回调不可用（on_error = closed）：错误
        let mut auth = AuthConfig::default();
        auth.authz_webhook = Some(WebhookConfig {
            url: "http://127.0.0.1:1/authz".into(),
            auth: ClientAuth::None,
            cache_ttl: Duration::from_secs(1),
            timeout: Duration::from_millis(200),
            fail_open: false,
            ..WebhookConfig::default()
        });
        let closed = Guard::build(
            GuardConfig {
                required: true,
                auth: auth.clone(),
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        closed.reload_policy().await.expect("load");
        assert!(matches!(
            closed.check(&alice, &Request::repo("x", Action::Read)).await,
            Err(AuthError::Unavailable(_))
        ));
        assert!(closed.exchange("scn_pat_x").await.is_err(), "unknown token");
        if let Some(w) = auth.authz_webhook.as_mut() {
            w.fail_open = true;
        }
        let open = Guard::build(
            GuardConfig {
                required: true,
                auth,
                signing_key_pem: None,
            },
            Arc::new(MemStore::new()),
            lookup(),
        )
        .expect("guard");
        open.reload_policy().await.expect("load");
        assert!(
            !open
                .check(&alice, &Request::repo("x", Action::Read))
                .await
                .expect("falls back to deny")
        );
    }

    #[test]
    fn build_rejects_bad_configuration() {
        let bad_key = GuardConfig {
            required: true,
            auth: AuthConfig::default(),
            signing_key_pem: Some("not a key".into()),
        };
        assert!(Guard::build(bad_key, Arc::new(MemStore::new()), lookup()).is_err());
        let auth = AuthConfig {
            issuers: vec![IssuerConfig {
                issuer: "x".into(),
                key: IssuerKey::Hs256(Secret {
                    env: Some("UNSET".into()),
                    ..Secret::default()
                }),
                audience: None,
                subject_claim: "sub".into(),
                groups_claim: None,
                repos_claim: None,
            }],
            ..AuthConfig::default()
        };
        let missing_secret = GuardConfig {
            required: true,
            auth,
            signing_key_pem: None,
        };
        assert!(Guard::build(missing_secret, Arc::new(MemStore::new()), lookup()).is_err());
    }

    #[test]
    fn error_conversions() {
        assert_eq!(AuthError::from(VerifyError::Expired), AuthError::Expired);
        assert!(matches!(
            AuthError::from(VerifyError::Unavailable("x".into())),
            AuthError::Unavailable(_)
        ));
        assert!(matches!(
            AuthError::from(scion_store::Error::Backend("db".into())),
            AuthError::Unavailable(_)
        ));
        assert!(AuthError::Missing.to_string().contains("required"));
    }
}
