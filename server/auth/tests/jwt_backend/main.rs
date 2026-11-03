//! Pins the JWT key and verify behavior that must not change when the jsonwebtoken backend changes.
//! The tests must pass on the rust_crypto backend and on the aws_lc_rs backend.

mod keys;

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rsa::{KeyPair as RsaKeyPair, KeySize, PublicKeyComponents};
use aws_lc_rs::signature::{
    ECDSA_P256_SHA256_FIXED_SIGNING, ECDSA_P384_SHA384_FIXED_SIGNING, EcdsaKeyPair, Ed25519KeyPair, KeyPair,
};
use axum::{Json, Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use scion_auth::config::{IssuerConfig, IssuerKey};
use scion_auth::external::{ExternalIssuer, VerifyError};
use scion_auth::token::{Issuer, KeyError};
use serde_json::{Value, json};

fn armor(tag: &str, der: &[u8]) -> String {
    let b64 = STANDARD.encode(der);
    let lines: Vec<&str> = b64
        .as_bytes()
        .chunks(64)
        .map(|c| std::str::from_utf8(c).expect("ascii"))
        .collect();
    format!("-----BEGIN {tag}-----\n{}\n-----END {tag}-----\n", lines.join("\n"))
}

fn b64(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

fn fixed_pem() -> String {
    armor("PRIVATE KEY", &STANDARD.decode(keys::P256_BODY).expect("fixture"))
}

// ---- Issuer::from_pem ----

#[test]
fn fixed_key_keeps_its_kid_and_coordinates() {
    let i = Issuer::from_pem(&fixed_pem(), "scion", 60).expect("fixed key");
    let jwk = i.jwk();
    assert_eq!(jwk.kid, keys::P256_KID);
    assert_eq!(jwk.x, keys::P256_X);
    assert_eq!(jwk.y, keys::P256_Y);
    assert_eq!(
        (jwk.kty.as_str(), jwk.crv.as_str(), jwk.alg.as_str()),
        ("EC", "P-256", "ES256")
    );
}

#[test]
fn fixed_key_signs_and_verifies() {
    let i = Issuer::from_pem(&fixed_pem(), "scion", 60).expect("fixed key");
    let (t, _) = i.issue("alice", &["g".to_owned()], now(), Some("sid1"), None);
    let c = i.verify(&t).expect("verify");
    assert_eq!((c.sub.as_str(), c.sid.as_deref()), ("alice", Some("sid1")));
    let h = jsonwebtoken::decode_header(&t).expect("header");
    assert_eq!((h.alg, h.kid.as_deref()), (Algorithm::ES256, Some(keys::P256_KID)));
}

#[test]
fn generated_key_loads() {
    let pem = scion_auth::token::generate_signing_key();
    assert!(pem.starts_with(&format!("-----BEGIN {}-----\n", "PRIVATE KEY")));
    Issuer::from_pem(&pem, "scion", 60).expect("generated key loads");
}

fn p384_pkcs8() -> Vec<u8> {
    let k = EcdsaKeyPair::generate(&ECDSA_P384_SHA384_FIXED_SIGNING).expect("gen");
    k.to_pkcs8v1().expect("pkcs8").as_ref().to_vec()
}

fn ed25519_pkcs8() -> Vec<u8> {
    let k = Ed25519KeyPair::generate().expect("gen");
    k.to_pkcs8v1().expect("pkcs8").as_ref().to_vec()
}

fn rsa_pkcs8() -> Vec<u8> {
    let k = RsaKeyPair::generate(KeySize::Rsa2048).expect("gen");
    k.as_der().expect("pkcs8").as_ref().to_vec()
}

#[test]
fn from_pem_denies_every_other_key_form() {
    let fixed = STANDARD.decode(keys::P256_BODY).expect("fixture");
    let rows: Vec<(&str, String)> = vec![
        ("not pem", "not a key".to_owned()),
        ("empty", String::new()),
        ("EC PRIVATE KEY tag", armor("EC PRIVATE KEY", &fixed)),
        ("PUBLIC KEY tag", armor("PUBLIC KEY", &fixed)),
        ("P-384", armor("PRIVATE KEY", &p384_pkcs8())),
        ("Ed25519", armor("PRIVATE KEY", &ed25519_pkcs8())),
        ("RSA", armor("PRIVATE KEY", &rsa_pkcs8())),
        ("junk DER", armor("PRIVATE KEY", b"junk junk junk")),
        ("two blocks", format!("{}{}", fixed_pem(), fixed_pem())),
    ];
    for (name, pem) in rows {
        let r = Issuer::from_pem(&pem, "scion", 60);
        assert!(matches!(r, Err(KeyError::Invalid(_))), "{name} must be denied");
    }
}

// ---- ExternalIssuer with a JWKS ----

struct Key {
    kid: &'static str,
    alg: Algorithm,
    enc: EncodingKey,
    jwk: Value,
}

fn ec_key(kid: &'static str, alg: Algorithm) -> Key {
    let (sa, crv, n) = match alg {
        Algorithm::ES256 => (&ECDSA_P256_SHA256_FIXED_SIGNING, "P-256", 32),
        _ => (&ECDSA_P384_SHA384_FIXED_SIGNING, "P-384", 48),
    };
    let k = EcdsaKeyPair::generate(sa).expect("gen");
    let pem = armor("PRIVATE KEY", k.to_pkcs8v1().expect("pkcs8").as_ref());
    let pt = k.public_key().as_ref().to_vec();
    Key {
        kid,
        alg,
        enc: EncodingKey::from_ec_pem(pem.as_bytes()).expect("enc"),
        jwk: json!({"kty":"EC","crv":crv,"kid":kid,"x":b64(&pt[1..1 + n]),"y":b64(&pt[1 + n..])}),
    }
}

fn rsa_key(kid: &'static str, alg: Algorithm) -> Key {
    let k = RsaKeyPair::generate(KeySize::Rsa2048).expect("gen");
    let pem = armor("PRIVATE KEY", k.as_der().expect("pkcs8").as_ref());
    let c = PublicKeyComponents::<Vec<u8>>::from(k.public_key());
    Key {
        kid,
        alg,
        enc: EncodingKey::from_rsa_pem(pem.as_bytes()).expect("enc"),
        jwk: json!({"kty":"RSA","kid":kid,"n":b64(&c.n),"e":b64(&c.e)}),
    }
}

fn sign(key: &Key, kid: &str) -> String {
    let mut h = Header::new(key.alg);
    h.kid = Some(kid.to_owned());
    let claims = json!({"iss":"idp","sub":"alice","exp":now() + 3600});
    jsonwebtoken::encode(&h, &claims, &key.enc).expect("sign")
}

async fn issuer_for(jwks: Value) -> ExternalIssuer {
    let app = Router::new().route("/jwks", get(move || async move { Json(jwks.clone()) }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}/jwks", l.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(l, app).await.expect("serve");
    });
    ExternalIssuer::new(
        IssuerConfig {
            issuer: "idp".into(),
            key: IssuerKey::Jwks(url),
            audience: None,
            subject_claim: "sub".into(),
            groups_claim: None,
            repos_claim: None,
        },
        reqwest::Client::new(),
        &|_| None,
    )
    .expect("issuer")
}

fn tamper(token: &str) -> String {
    let (head, sig) = token.rsplit_once('.').expect("jws");
    let mut s: Vec<char> = sig.chars().collect();
    s[0] = if s[0] == 'A' { 'B' } else { 'A' };
    format!("{head}.{}", s.into_iter().collect::<String>())
}

#[tokio::test]
async fn external_verify_accepts_each_algorithm_and_denies_forgeries() {
    let rs = rsa_key("rs", Algorithm::RS256);
    let ps = Key {
        kid: "ps",
        alg: Algorithm::PS256,
        enc: rs.enc.clone(),
        jwk: {
            let mut j = rs.jwk.clone();
            j["kid"] = json!("ps");
            j
        },
    };
    let es256 = ec_key("es256", Algorithm::ES256);
    let es384 = ec_key("es384", Algorithm::ES384);
    let other_rsa = rsa_key("other", Algorithm::RS256);
    let other_ec = ec_key("other-ec", Algorithm::ES256);
    let idp = issuer_for(json!({"keys":[rs.jwk, ps.jwk, es256.jwk, es384.jwk]})).await;

    for k in [&rs, &ps, &es256, &es384] {
        let p = idp.verify(&sign(k, k.kid)).await;
        assert!(p.is_ok(), "{:?} must be accepted: {p:?}", k.alg);
    }

    let denied = |name: &str, r: Result<_, VerifyError>| {
        assert!(r.is_err(), "{name} must be denied");
    };
    for k in [&rs, &ps, &es256, &es384] {
        denied("changed signature", idp.verify(&tamper(&sign(k, k.kid))).await);
    }
    denied(
        "RSA token from another RSA key",
        idp.verify(&sign(&other_rsa, "rs")).await,
    );
    denied(
        "PS token from another RSA key",
        idp.verify(&sign(
            &Key {
                alg: Algorithm::PS256,
                ..other_rsa
            },
            "ps",
        ))
        .await,
    );
    denied(
        "ES256 token from another EC key",
        idp.verify(&sign(&other_ec, "es256")).await,
    );
    denied(
        "algorithm mismatch: ES256 token under the RSA kid",
        idp.verify(&sign(&es256, "rs")).await,
    );

    let hs = jsonwebtoken::encode(
        &{
            let mut h = Header::new(Algorithm::HS256);
            h.kid = Some("rs".into());
            h
        },
        &json!({"iss":"idp","sub":"alice","exp":now() + 3600}),
        &EncodingKey::from_secret(b"secret"),
    )
    .expect("hs");
    denied("HS256", idp.verify(&hs).await);

    let claims = b64(json!({"iss":"idp","sub":"alice","exp":now() + 3600})
        .to_string()
        .as_bytes());
    let none = format!("{}.{claims}.", b64(br#"{"alg":"none","kid":"rs"}"#));
    denied("alg none", idp.verify(&none).await);
}
