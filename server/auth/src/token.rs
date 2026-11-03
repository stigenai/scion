//! 令牌：服务端自签的短期 JWT（ES256），以及不透明令牌（PAT / service token / 刷新令牌）。
//!
//! - JWT claims：`iss`、`sub`、`groups`、`iat`、`exp`，以及可选的 `sid`（来源令牌记录的 id）。签名私钥是 PKCS#8 PEM 格式的 P-256 密钥，
//!   集群中所有节点使用同一个（`scion admin jwt-keygen` 生成）。
//! - 不透明令牌形如 `scn_pat_<43 个 base64url 字符>`，服务端只保存它的 SHA-256；公开标识是哈希的
//!   前 12 个十六进制字符，用于列出和吊销。

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// 不透明令牌的前缀。
pub const OPAQUE_PREFIX: &str = "scn_";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("invalid signing key: {0}")]
    Invalid(String),
}

/// 生成一个新的 ES256 签名私钥（PKCS#8 PEM）。
pub fn generate_signing_key() -> String {
    let key = p256::SecretKey::random(&mut OsRng);
    match key.to_pkcs8_pem(LineEnding::LF) {
        Ok(pem) => pem.to_string(),
        // P-256 私钥的 PKCS#8 编码不会失败
        Err(e) => unreachable!("encoding a P-256 key: {e}"),
    }
}

/// 服务端自签 JWT 的 claims。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub sub: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    pub iat: i64,
    pub exp: i64,
    /// Id of the source token record (PAT or refresh token) this JWT was exchanged from.
    /// The JWT is valid only while that record exists and has not expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
}

/// JWKS 中的一个公钥。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub crv: String,
    pub x: String,
    pub y: String,
    pub kid: String,
    #[serde(default)]
    pub alg: String,
    #[serde(rename = "use", default)]
    pub use_: String,
}

/// 签发与验证自签 JWT。
pub struct Issuer {
    issuer: String,
    kid: String,
    encoding: EncodingKey,
    decoding: DecodingKey,
    jwk: Jwk,
    ttl_secs: i64,
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer")
            .field("issuer", &self.issuer)
            .field("kid", &self.kid)
            .finish()
    }
}

impl Issuer {
    /// `pem` 为 PKCS#8 格式的 P-256 私钥；`issuer` 写入 `iss`；`ttl_secs` 为令牌有效期。
    pub fn from_pem(pem: &str, issuer: &str, ttl_secs: i64) -> Result<Self, KeyError> {
        let secret = p256::SecretKey::from_pkcs8_pem(pem).map_err(|e| KeyError::Invalid(e.to_string()))?;
        let point = p256::elliptic_curve::sec1::ToEncodedPoint::to_encoded_point(&secret.public_key(), false);
        let (x, y) = match (point.x(), point.y()) {
            (Some(x), Some(y)) => (URL_SAFE_NO_PAD.encode(x), URL_SAFE_NO_PAD.encode(y)),
            _ => return Err(KeyError::Invalid("public key is the identity point".into())),
        };
        // kid：公钥坐标的哈希，换密钥时自然不同
        let kid = hex::encode(&Sha256::digest(format!("{x}.{y}").as_bytes())[..8]);
        let encoding = EncodingKey::from_ec_pem(pem.as_bytes()).map_err(|e| KeyError::Invalid(e.to_string()))?;
        let decoding = DecodingKey::from_ec_components(&x, &y).map_err(|e| KeyError::Invalid(e.to_string()))?;
        Ok(Self {
            issuer: issuer.to_owned(),
            jwk: Jwk {
                kty: "EC".into(),
                crv: "P-256".into(),
                x,
                y,
                kid: kid.clone(),
                alg: "ES256".into(),
                use_: "sig".into(),
            },
            kid,
            encoding,
            decoding,
            ttl_secs: ttl_secs.max(1),
        })
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn jwk(&self) -> &Jwk {
        &self.jwk
    }

    /// 签发访问令牌，返回令牌与过期时间（Unix 毫秒）。
    /// `sid` is the id of the opaque token this JWT is exchanged from; it sets the `sid` claim.
    /// `not_after_ms` (Unix ms) cuts `exp` to the whole second at or before it.
    pub fn issue(
        &self,
        subject: &str,
        groups: &[String],
        now_secs: i64,
        sid: Option<&str>,
        not_after_ms: Option<i64>,
    ) -> (String, i64) {
        let mut exp = now_secs + self.ttl_secs;
        if let Some(src_ms) = not_after_ms {
            exp = exp.min(src_ms.div_euclid(1000));
        }
        let claims = Claims {
            iss: self.issuer.clone(),
            sub: subject.to_owned(),
            groups: groups.to_vec(),
            iat: now_secs,
            exp,
            sid: sid.map(str::to_owned),
        };
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.kid.clone());
        match jsonwebtoken::encode(&header, &claims, &self.encoding) {
            Ok(t) => (t, claims.exp * 1000),
            // 用已校验过的密钥签名不会失败
            Err(e) => unreachable!("signing a JWT with a valid key: {e}"),
        }
    }

    /// 验证自签令牌（签名、`iss`、过期时间）。
    pub fn verify(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let mut v = Validation::new(Algorithm::ES256);
        v.set_issuer(&[&self.issuer]);
        v.leeway = 30;
        v.validate_aud = false;
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &v).map(|d| d.claims)
    }
}

/// 新的不透明令牌：(令牌, 公开标识, 哈希)。`kind` 为 `pat` 或 `refresh`。
pub fn new_opaque(kind: &str) -> (String, String, [u8; 32]) {
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    let token = format!("{OPAQUE_PREFIX}{kind}_{}", URL_SAFE_NO_PAD.encode(raw));
    let hash = hash_opaque(&token);
    (token, opaque_id(&hash), hash)
}

pub fn hash_opaque(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// 公开标识：哈希的前 12 个十六进制字符。
pub fn opaque_id(hash: &[u8; 32]) -> String {
    hex::encode(&hash[..6])
}

/// 不验证签名，读取 JWT 的 `iss` 与 header 中的 `kid`（用来决定用哪把公钥验证）。
pub fn peek(token: &str) -> Option<(Option<String>, Option<String>)> {
    let header = jsonwebtoken::decode_header(token).ok()?;
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some((v.get("iss").and_then(|i| i.as_str()).map(str::to_owned), header.kid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issuer() -> Issuer {
        Issuer::from_pem(&generate_signing_key(), "https://scion.test", 900).expect("issuer")
    }

    #[test]
    fn issue_and_verify_round_trip() {
        let i = issuer();
        let now = 1_700_000_000;
        let (t, exp) = i.issue("alice", &["eng".into()], now, None, None);
        assert_eq!(exp, (now + 900) * 1000);
        let real_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let (fresh, _) = i.issue("alice", &["eng".into()], real_now, None, None);
        let c = i.verify(&fresh).expect("valid");
        assert_eq!(
            (c.sub.as_str(), c.groups.as_slice(), c.iss.as_str()),
            ("alice", &["eng".to_string()][..], "https://scion.test")
        );
        assert!(i.verify(&t).is_err(), "expired long ago");
        assert_eq!(
            peek(&fresh),
            Some((Some("https://scion.test".into()), Some(i.jwk().kid.clone())))
        );
        assert_eq!(i.issuer(), "https://scion.test");
        assert!(format!("{i:?}").contains("kid"));
    }

    #[test]
    fn source_binding_sets_sid_and_clamps_exp() {
        let i = issuer();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        // Source ends in 60 s 500 ms: exp is cut to the whole second before it.
        let src_ms = (now + 60) * 1000 + 500;
        let (t, exp) = i.issue("alice", &[], now, Some("abc"), Some(src_ms));
        assert_eq!(exp, (now + 60) * 1000);
        assert_eq!(i.verify(&t).expect("valid").sid.as_deref(), Some("abc"));
        // A source that outlives the default lifetime does not extend it.
        let (_, exp) = i.issue("alice", &[], now, Some("abc"), Some(src_ms + 3_600_000));
        assert_eq!(exp, (now + 900) * 1000);
        // A source without an expiry keeps the default lifetime.
        let (_, exp) = i.issue("alice", &[], now, Some("abc"), None);
        assert_eq!(exp, (now + 900) * 1000);
        // A JWT without a source has no sid.
        let (t, _) = i.issue("alice", &[], now, None, None);
        assert_eq!(i.verify(&t).expect("valid").sid, None);
    }

    #[test]
    fn other_keys_and_issuers_are_rejected() {
        let a = issuer();
        let b = issuer();
        assert_ne!(a.jwk().kid, b.jwk().kid);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let (t, _) = b.issue("mallory", &[], now, None, None);
        assert!(a.verify(&t).is_err(), "signed by another key");
        let other_iss = Issuer::from_pem(&generate_signing_key(), "https://elsewhere", 60).expect("issuer");
        let (t, _) = other_iss.issue("x", &[], now, None, None);
        assert!(a.verify(&t).is_err());
        assert!(a.verify("not.a.jwt").is_err());
        assert_eq!(peek("garbage"), None);
    }

    #[test]
    fn bad_keys_are_rejected() {
        assert!(Issuer::from_pem("not a key", "i", 60).is_err());
        let rsa_like = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
        assert!(Issuer::from_pem(rsa_like, "i", 60).is_err());
    }

    #[test]
    fn opaque_tokens() {
        let (t, id, hash) = new_opaque("pat");
        assert!(t.starts_with("scn_pat_"));
        assert_eq!(t.len(), "scn_pat_".len() + 43);
        assert_eq!(hash, hash_opaque(&t));
        assert_eq!(id, opaque_id(&hash));
        assert_eq!(id.len(), 12);
        let (t2, ..) = new_opaque("pat");
        assert_ne!(t, t2);
    }
}
