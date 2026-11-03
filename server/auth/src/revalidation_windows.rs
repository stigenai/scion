//! One test per revalidation window. Each test revokes a credential or changes a policy.
//! It then reads inside the window and after it. ADR 0003 in the stek repo records the windows.

use std::sync::Arc;
use std::time::Duration;

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use scion_store::{MemStore, MetaStore};
use serde_json::{Value, json};

use crate::config::{AuthConfig, ClientAuth, IntrospectionConfig, Secret, WebhookConfig};
use crate::guard::{AuthError, Guard, GuardConfig};
use crate::policy::{Action, Principal, Request};
use crate::remote::{AuthzWebhook, Introspection, env_lookup};
use crate::testing::FakeIdp;

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

fn basic() -> ClientAuth {
    ClientAuth::Basic {
        client_id: "scion".into(),
        secret: Secret {
            value: Some("s3cret".into()),
            ..Secret::default()
        },
    }
}

fn introspection(idp: &FakeIdp, ttl: Duration) -> Introspection {
    Introspection::new(
        IntrospectionConfig {
            url: idp.url("/introspect"),
            auth: basic(),
            cache_ttl: ttl,
            subject_claim: "username".into(),
            groups_claim: None,
        },
        reqwest::Client::new(),
        env_lookup(),
    )
}

fn active(exp: Option<i64>) -> Value {
    match exp {
        Some(e) => json!({"active": true, "username": "alice", "exp": e}),
        None => json!({"active": true, "username": "alice"}),
    }
}

// ---------- AC1: introspection cache ----------

#[tokio::test]
async fn introspection_revocation_is_seen_after_cache_ttl_and_not_before() {
    let idp = FakeIdp::start().await;
    idp.set_introspection("t1", active(None));
    let i = introspection(&idp, Duration::from_secs(1));
    assert!(i.verify("t1").await.expect("ok").is_some());
    idp.set_introspection("t1", json!({"active": false}));
    assert!(
        i.verify("t1").await.expect("ok").is_some(),
        "inside cache_ttl the cached answer stands"
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(i.verify("t1").await, Ok(None), "after cache_ttl the IdP is asked again");
}

#[tokio::test]
async fn introspection_cache_ttl_zero_turns_the_cache_off() {
    let idp = FakeIdp::start().await;
    idp.set_introspection("t1", active(None));
    let i = introspection(&idp, Duration::ZERO);
    assert!(i.verify("t1").await.expect("ok").is_some());
    idp.set_introspection("t1", json!({"active": false}));
    assert_eq!(i.verify("t1").await, Ok(None), "revocation shows on the next call");
    assert_eq!(idp.calls("/introspect"), 2);
}

#[tokio::test]
async fn introspection_entry_ends_at_token_expiry() {
    let idp = FakeIdp::start().await;
    idp.set_introspection("t1", active(Some(now_secs() + 1)));
    let i = introspection(&idp, Duration::from_secs(60));
    assert!(i.verify("t1").await.expect("ok").is_some());
    idp.set_introspection("t1", json!({"active": false}));
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(i.verify("t1").await, Ok(None), "the entry did not outlive exp");
    assert_eq!(idp.calls("/introspect"), 2);
}

#[tokio::test]
async fn introspection_refuses_an_active_answer_with_a_past_exp() {
    let idp = FakeIdp::start().await;
    idp.set_introspection("t1", active(Some(now_secs() - 5)));
    let i = introspection(&idp, Duration::from_secs(60));
    assert_eq!(i.verify("t1").await, Ok(None));
}

#[tokio::test]
async fn introspection_still_caches_future_and_missing_exp() {
    let idp = FakeIdp::start().await;
    idp.set_introspection("future", active(Some(now_secs() + 3600)));
    idp.set_introspection("none", active(None));
    let i = introspection(&idp, Duration::from_secs(60));
    for t in ["future", "none"] {
        assert!(i.verify(t).await.expect("ok").is_some());
        assert!(i.verify(t).await.expect("ok").is_some());
    }
    assert_eq!(idp.calls("/introspect"), 2, "one call per token");
}

// ---------- AC2: authz webhook cache ----------

fn webhook(idp: &FakeIdp, ttl: Duration) -> AuthzWebhook {
    AuthzWebhook::new(
        WebhookConfig {
            url: idp.url("/authz"),
            auth: ClientAuth::Bearer(Secret {
                value: Some("opa".into()),
                ..Secret::default()
            }),
            cache_ttl: ttl,
            timeout: Duration::from_secs(2),
            ..WebhookConfig::default()
        },
        reqwest::Client::new(),
        env_lookup(),
    )
}

#[tokio::test]
async fn webhook_policy_change_is_seen_after_cache_ttl_and_not_before() {
    let idp = FakeIdp::start().await;
    let w = webhook(&idp, Duration::from_secs(1));
    let alice = Principal::user("alice", vec![]);
    let read = Request::repo("models/a", Action::Read);
    assert_eq!(w.allows(&alice, &read).await, Ok(true));
    idp.set_authz(json!({"result": false}));
    assert_eq!(w.allows(&alice, &read).await, Ok(true), "inside cache_ttl");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(w.allows(&alice, &read).await, Ok(false), "after cache_ttl");
}

#[tokio::test]
async fn webhook_cache_ttl_zero_turns_the_allow_cache_off() {
    let idp = FakeIdp::start().await;
    let w = webhook(&idp, Duration::ZERO);
    let alice = Principal::user("alice", vec![]);
    let read = Request::repo("models/a", Action::Read);
    assert_eq!(w.allows(&alice, &read).await, Ok(true));
    idp.set_authz(json!({"result": false}));
    assert_eq!(w.allows(&alice, &read).await, Ok(false));
}

#[tokio::test]
async fn webhook_deny_stays_cached_for_five_seconds() {
    let idp = FakeIdp::start().await;
    let w = webhook(&idp, Duration::from_secs(30));
    let alice = Principal::user("alice", vec![]);
    let write = Request::repo("models/a", Action::Write);
    assert_eq!(w.allows(&alice, &write).await, Ok(false));
    idp.set_authz(json!({"result": true}));
    assert_eq!(w.allows(&alice, &write).await, Ok(false), "the deny is still cached");
}

// ---------- AC5: exchange ----------

fn external_jwt(exp: i64) -> String {
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({"current_user": "carol", "iss": "OpenCSG", "exp": exp}),
        &EncodingKey::from_secret(b"signing-key"),
    )
    .expect("sign")
}

fn guard(idp: &FakeIdp, introspection_ttl: &str) -> Arc<Guard> {
    let auth = AuthConfig::parse(&format!(
        r#"
[token]
issuer = "https://scion.test"

[[issuer]]
issuer = "OpenCSG"
hs256_secret = "signing-key"
subject_claim = "current_user"

[introspection]
url = "{}"
client_id = "scion"
client_secret = "s3cret"
cache_ttl = "{introspection_ttl}"
"#,
        idp.url("/introspect")
    ))
    .expect("auth config");
    let meta: Arc<dyn MetaStore> = Arc::new(MemStore::new());
    Guard::build(
        GuardConfig {
            required: true,
            auth,
            signing_key_pem: Some(crate::token::generate_signing_key()),
        },
        meta,
        env_lookup(),
    )
    .expect("guard")
}

#[tokio::test]
async fn exchanged_jwt_from_an_external_jwt_ends_with_its_source() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    let src = now_secs() + 60;
    let (_, exp_ms, _) = g.exchange(&external_jwt(src)).await.expect("exchange");
    assert!(exp_ms <= src * 1000, "exp {exp_ms} is past the source exp {src}");
    let src = now_secs() + 3600;
    let (_, exp_ms, _) = g.exchange(&external_jwt(src)).await.expect("exchange");
    let cap = (now_secs() + 900) * 1000;
    assert!(
        (cap - 2000..=cap).contains(&exp_ms),
        "the 900 s cap stays a minimum: {exp_ms}"
    );
}

#[tokio::test]
async fn exchanged_jwt_from_an_introspected_token_ends_with_the_cache_ttl() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    let src = now_secs() + 30;
    idp.set_introspection("opaque-ext", active(Some(src)));
    let (_, exp_ms, _) = g.exchange("opaque-ext").await.expect("exchange");
    assert!(exp_ms <= src * 1000, "never past the token's exp");
    idp.set_introspection("opaque-ext2", active(Some(now_secs() + 3600)));
    let (_, exp_ms, _) = g.exchange("opaque-ext2").await.expect("exchange");
    let cap = (now_secs() + 60) * 1000;
    assert!(
        (cap - 3000..=cap).contains(&exp_ms),
        "now + cache_ttl is the limit: {exp_ms}"
    );
}

#[tokio::test]
async fn exchange_refuses_a_source_that_ended_inside_the_verifier_leeway() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    assert!(matches!(
        g.exchange(&external_jwt(now_secs() - 10)).await,
        Err(AuthError::Expired)
    ));
}

#[tokio::test]
async fn exchange_keeps_opaque_binding_and_refuses_own_jwt() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    let (pat, _) = g
        .create_token("pat", "alice", vec![], "", Some(Duration::from_secs(120)))
        .await
        .expect("pat");
    let (jwt, exp_ms, _) = g.exchange(&pat).await.expect("exchange");
    assert!(exp_ms <= (now_secs() + 120) * 1000);
    assert_eq!(g.authenticate(Some(&jwt)).await.expect("bound").subject, "alice");
    assert!(matches!(g.exchange(&jwt).await, Err(AuthError::Invalid(_))));
}

#[tokio::test]
async fn exchange_caps_a_jwt_shaped_token_that_no_issuer_knows() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    idp.set_introspection("aa.bb.cc", active(Some(now_secs() + 3600)));
    let (_, exp_ms, _) = g.exchange("aa.bb.cc").await.expect("exchange");
    let cap = (now_secs() + 60) * 1000;
    assert!(
        (cap - 3000..=cap).contains(&exp_ms),
        "now + cache_ttl is the limit: {exp_ms}"
    );
}

#[tokio::test]
async fn exchange_reads_introspection_fresh_not_from_the_cache() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "60s");
    idp.set_introspection("opaque-ext", active(Some(now_secs() + 3600)));
    g.authenticate(Some("opaque-ext")).await.expect("cached");
    idp.set_introspection("opaque-ext", json!({"active": false}));
    assert!(matches!(g.exchange("opaque-ext").await, Err(AuthError::Invalid(_))));
}

#[tokio::test]
async fn exchange_from_an_introspected_token_is_refused_when_the_cache_is_off() {
    let idp = FakeIdp::start().await;
    let g = guard(&idp, "0s");
    idp.set_introspection("opaque-ext", active(Some(now_secs() + 3600)));
    assert!(matches!(g.exchange("opaque-ext").await, Err(AuthError::Unavailable(_))));
}
