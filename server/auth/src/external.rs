//! 验证受信任的外部签发方的 JWT：非对称密钥从 JWKS 获取（标准 OIDC 提供方），或 HS256 共享密钥
//! （例如 starhub-server）。claims 到身份的映射可配置。

use std::sync::RwLock;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde_json::Value;

use crate::config::{IssuerConfig, IssuerKey};
use crate::policy::{Action, Grant, Principal};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("token expired")]
    Expired,
    #[error("invalid token: {0}")]
    Invalid(String),
    /// 取 JWKS 等外部依赖失败（可重试）。
    #[error("identity provider unavailable: {0}")]
    Unavailable(String),
}

impl From<jsonwebtoken::errors::Error> for VerifyError {
    fn from(e: jsonwebtoken::errors::Error) -> Self {
        match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => VerifyError::Expired,
            _ => VerifyError::Invalid(e.to_string()),
        }
    }
}

/// 允许从 JWKS 使用的算法（不含 HS*：对称算法不能从公开的 JWKS 取密钥）。
const ASYMMETRIC: [Algorithm; 8] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
];

/// JWKS 缓存：过期或遇到未知的 kid 时重新获取（两次获取至少间隔 `min_refresh`）。
pub struct Jwks {
    url: String,
    http: reqwest::Client,
    state: RwLock<Option<(JwkSet, Instant)>>,
    ttl: Duration,
    min_refresh: Duration,
}

impl Jwks {
    pub fn new(url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            url: url.into(),
            http,
            state: RwLock::new(None),
            ttl: Duration::from_secs(600),
            min_refresh: Duration::from_secs(30),
        }
    }

    async fn fetch(&self) -> Result<JwkSet, VerifyError> {
        let resp = self
            .http
            .get(&self.url)
            .send()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("fetch {}: {e}", self.url)))?;
        if !resp.status().is_success() {
            return Err(VerifyError::Unavailable(format!(
                "fetch {}: HTTP {}",
                self.url,
                resp.status()
            )));
        }
        resp.json::<JwkSet>()
            .await
            .map_err(|e| VerifyError::Unavailable(format!("parse JWKS from {}: {e}", self.url)))
    }

    /// 按 kid 取解码密钥。
    pub async fn key(&self, kid: Option<&str>) -> Result<DecodingKey, VerifyError> {
        let (cached, age) = match self.state.read() {
            Ok(s) => match s.as_ref() {
                Some((set, at)) => (Some(set.clone()), Some(at.elapsed())),
                None => (None, None),
            },
            Err(_) => (None, None),
        };
        let find = |set: &JwkSet| -> Option<DecodingKey> {
            let jwk = match kid {
                Some(k) => set.find(k),
                None if set.keys.len() == 1 => set.keys.first(),
                None => None,
            }?;
            DecodingKey::from_jwk(jwk).ok()
        };
        if let (Some(set), Some(age)) = (&cached, age)
            && age < self.ttl
        {
            if let Some(k) = find(set) {
                return Ok(k);
            }
            // 未知的 kid：密钥可能刚轮换过，但不要每个请求都去取
            if age < self.min_refresh {
                return Err(VerifyError::Invalid(format!("unknown key id {kid:?}")));
            }
        }
        let set = self.fetch().await?;
        let key = find(&set);
        if let Ok(mut s) = self.state.write() {
            *s = Some((set, Instant::now()));
        }
        key.ok_or_else(|| VerifyError::Invalid(format!("unknown key id {kid:?}")))
    }
}

/// 一个受信任的外部签发方。
pub struct ExternalIssuer {
    config: IssuerConfig,
    key: KeySource,
}

enum KeySource {
    Jwks(Jwks),
    Secret(DecodingKey),
}

impl ExternalIssuer {
    /// `lookup` 读环境变量（用于 HS256 密钥）。
    pub fn new(
        config: IssuerConfig,
        http: reqwest::Client,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, crate::config::ConfigError> {
        let key = match &config.key {
            IssuerKey::Jwks(url) => KeySource::Jwks(Jwks::new(url.clone(), http)),
            IssuerKey::Hs256(secret) => KeySource::Secret(DecodingKey::from_secret(secret.resolve(lookup)?.as_bytes())),
        };
        Ok(Self { config, key })
    }

    pub fn issuer(&self) -> &str {
        &self.config.issuer
    }

    /// 验证令牌并映射为身份。
    pub async fn verify(&self, token: &str) -> Result<Principal, VerifyError> {
        let header = jsonwebtoken::decode_header(token)?;
        let (key, algs) = match &self.key {
            KeySource::Secret(k) => (k.clone(), vec![Algorithm::HS256]),
            KeySource::Jwks(j) => {
                if !ASYMMETRIC.contains(&header.alg) {
                    return Err(VerifyError::Invalid(format!(
                        "algorithm {:?} is not allowed",
                        header.alg
                    )));
                }
                (j.key(header.kid.as_deref()).await?, vec![header.alg])
            }
        };
        let mut v = Validation::new(algs[0]);
        v.algorithms = algs;
        v.set_issuer(&[&self.config.issuer]);
        v.leeway = 30;
        match &self.config.audience {
            Some(aud) => v.set_audience(&[aud]),
            None => v.validate_aud = false,
        }
        let claims = jsonwebtoken::decode::<Value>(token, &key, &v)?.claims;
        principal_from_claims(
            &claims,
            &self.config.subject_claim,
            self.config.groups_claim.as_deref(),
            self.config.repos_claim.as_deref(),
        )
    }
}

/// 从 claims（JWT 或 introspection 响应）构造身份。
pub fn principal_from_claims(
    claims: &Value,
    subject_claim: &str,
    groups_claim: Option<&str>,
    repos_claim: Option<&str>,
) -> Result<Principal, VerifyError> {
    let subject = claims
        .get(subject_claim)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| VerifyError::Invalid(format!("claim {subject_claim:?} is missing")))?;
    let groups = groups_claim
        .and_then(|c| claims.get(c))
        .map(|g| match g {
            Value::Array(a) => a.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
            Value::String(s) => s
                .split([',', ' '])
                .filter(|x| !x.is_empty())
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        })
        .unwrap_or_default();
    let grants = repos_claim
        .and_then(|c| claims.get(c))
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(repo, a)| {
                    Some(Grant {
                        repo: repo.clone(),
                        action: Action::parse(a.as_str()?)?,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut p = Principal::user(subject, groups);
    p.grants = grants;
    p.expires_at_ms = claims.get("exp").and_then(Value::as_i64).map(|e| e * 1000);
    Ok(p)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::Secret;
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::json;

    pub(crate) fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64
    }

    fn hs_issuer(subject_claim: &str) -> ExternalIssuer {
        ExternalIssuer::new(
            IssuerConfig {
                issuer: "OpenCSG".into(),
                key: IssuerKey::Hs256(Secret {
                    value: Some("signing-key".into()),
                    env: None,
                    file: None,
                }),
                audience: None,
                subject_claim: subject_claim.into(),
                groups_claim: None,
                repos_claim: None,
            },
            reqwest::Client::new(),
            &|_| None,
        )
        .expect("issuer")
    }

    fn hs_token(claims: Value, secret: &str) -> String {
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("sign")
    }

    /// 与 starhub-server 签发的 JWT 相同的形状：HS256、iss=OpenCSG、用户名在 current_user。
    #[tokio::test]
    async fn starhub_style_hs256_tokens() {
        let i = hs_issuer("current_user");
        assert_eq!(i.issuer(), "OpenCSG");
        let t = hs_token(
            json!({"uuid": "u-1", "current_user": "alice", "iss": "OpenCSG", "iat": now(), "exp": now() + 3600}),
            "signing-key",
        );
        let p = i.verify(&t).await.expect("valid");
        assert_eq!((p.subject.as_str(), p.authenticated), ("alice", true));
        assert_eq!(p.expires_at_ms, Some((now() + 3600) * 1000));
        // 密钥不对、issuer 不对、过期
        let wrong_key = hs_token(
            json!({"current_user": "a", "iss": "OpenCSG", "exp": now() + 60}),
            "other",
        );
        assert!(matches!(i.verify(&wrong_key).await, Err(VerifyError::Invalid(_))));
        let wrong_iss = hs_token(
            json!({"current_user": "a", "iss": "x", "exp": now() + 60}),
            "signing-key",
        );
        assert!(i.verify(&wrong_iss).await.is_err());
        let expired = hs_token(
            json!({"current_user": "a", "iss": "OpenCSG", "exp": now() - 3600}),
            "signing-key",
        );
        assert_eq!(i.verify(&expired).await, Err(VerifyError::Expired));
        // 缺少身份 claim
        let no_user = hs_token(json!({"iss": "OpenCSG", "exp": now() + 60}), "signing-key");
        assert!(i.verify(&no_user).await.is_err());
        assert!(i.verify("garbage").await.is_err());
    }

    #[test]
    fn claims_mapping() {
        let c = json!({
            "sub": "u1", "groups": ["eng", "ops"], "roles": "a, b",
            "repos": {"models/x": "write", "bad": "own", "y": 3}, "exp": 10
        });
        let p = principal_from_claims(&c, "sub", Some("groups"), Some("repos")).expect("ok");
        assert_eq!(p.groups, ["eng", "ops"]);
        assert_eq!(
            p.grants,
            [Grant {
                repo: "models/x".into(),
                action: Action::Write
            }]
        );
        assert_eq!(p.expires_at_ms, Some(10_000));
        let p = principal_from_claims(&c, "sub", Some("roles"), None).expect("ok");
        assert_eq!(p.groups, ["a", "b"]);
        let p = principal_from_claims(&c, "sub", Some("exp"), Some("missing")).expect("ok");
        assert!(p.groups.is_empty() && p.grants.is_empty());
        assert!(principal_from_claims(&json!({"sub": ""}), "sub", None, None).is_err());
    }
}
