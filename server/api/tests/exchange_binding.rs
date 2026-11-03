//! An access JWT that this server exchanged from a PAT or refresh token must stop working when the source token
//! is revoked or expires, on every replica, for unary reads and for the next WatchRefs batch.
//! Two replicas (A and B) share one MemStore and one signing key.

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client};
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{ResolveRefRequest, WatchRefsRequest, WatchRefsResponse};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::{Code, Streaming};

const POLICY: &str = r#"
[[rule]]
subjects = ["group:eng"]
repos = ["mono"]
actions = ["land"]

[[rule]]
subjects = ["user:carol"]
repos = ["mono"]
actions = ["read"]
"#;

const AUTH: &str = r#"
[token]
issuer = "https://scion.test"

[[issuer]]
issuer = "OpenCSG"
hs256_secret = "signing-key"
subject_claim = "current_user"
"#;

type Refs = RefServiceClient<tonic::transport::Channel>;

struct Replica {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn replica(store: &Arc<MemStore>, key: &str) -> Replica {
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(AUTH).expect("auth config"),
        signing_key_pem: Some(key.to_owned()),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            ref_poll_interval: Duration::from_millis(50),
            stream_recheck_interval: Duration::from_millis(200),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store.clone(),
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Replica { url, guard, _stop: tx }
}

async fn replicas() -> (Replica, Replica) {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let key = scion_api::scion_auth::token::generate_signing_key();
    let a = replica(&store, &key).await;
    let b = replica(&store, &key).await;
    a.guard.set_policy(POLICY).await.expect("policy");
    b.guard.reload_policy().await.expect("policy on b");
    (a, b)
}

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn pat(r: &Replica, subject: &str, groups: &[&str], ttl: Option<Duration>) -> (String, String, Option<i64>) {
    let (t, rec) = r
        .guard
        .create_token(
            "pat",
            subject,
            groups.iter().map(|g| (*g).to_string()).collect(),
            "test",
            ttl,
        )
        .await
        .expect("token");
    (t, rec.id, rec.expires_at_ms)
}

async fn resolve(url: &str, token: &str) -> Result<(), Code> {
    let mut refs: Refs = RefServiceClient::connect(url.to_owned()).await.expect("connect");
    refs.resolve_ref(with_token(
        ResolveRefRequest {
            repo: "mono".into(),
            r#ref: "main".into(),
        },
        token,
    ))
    .await
    .map(|_| ())
    .map_err(|e| e.code())
}

async fn land_change(writer: &Client, n: usize) {
    let tip = writer.resolve_ref("main").await.expect("resolve").map(|t| t.changeset);
    let cs = writer
        .changeset(tip)
        .put("f.txt", format!("v{n}"))
        .create()
        .await
        .expect("create");
    writer.land("main", &cs.id).await.expect("land");
}

async fn writer(a: &Replica) -> Client {
    let (eng, _, _) = pat(a, "bob", &["eng"], None).await;
    let w = Client::builder()
        .endpoint(&a.url)
        .repo("mono")
        .auth(Auth::Bearer(eng))
        .build()
        .await
        .expect("connect");
    land_change(&w, 1).await;
    w
}

async fn open_watch(url: &str, token: &str) -> Streaming<WatchRefsResponse> {
    let mut refs: Refs = RefServiceClient::connect(url.to_owned()).await.expect("connect");
    let mut stream = refs
        .watch_refs(with_token(
            WatchRefsRequest {
                repo: "mono".into(),
                refs: vec!["main".into()],
                known_versions: Default::default(),
            },
            token,
        ))
        .await
        .expect("watch")
        .into_inner();
    assert!(stream.message().await.expect("snapshot").expect("msg").snapshot);
    stream
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_exchanged_jwt_reads_on_both_replicas() {
    let (a, b) = replicas().await;
    let (p, _, _) = pat(&a, "carol", &[], None).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    writer(&a).await;
    assert_eq!(resolve(&a.url, &jwt).await, Ok(()));
    assert_eq!(resolve(&b.url, &jwt).await, Ok(()));
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_the_source_pat_on_b_stops_the_jwt_on_both_replicas() {
    let (a, b) = replicas().await;
    let (p, id, _) = pat(&a, "carol", &[], None).await;
    let (other, _, _) = pat(&a, "dave", &["eng"], None).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    let (other_jwt, _, _) = a.guard.exchange(&other).await.expect("exchange other");
    writer(&a).await;
    assert_eq!(resolve(&a.url, &jwt).await, Ok(()));
    assert!(b.guard.revoke_token(&id).await.expect("revoke"));
    assert_eq!(resolve(&a.url, &jwt).await, Err(Code::Unauthenticated));
    assert_eq!(resolve(&b.url, &jwt).await, Err(Code::Unauthenticated));
    // Controls: another user's exchanged JWT still works on both replicas.
    assert_eq!(resolve(&a.url, &other_jwt).await, Ok(()));
    assert_eq!(resolve(&b.url, &other_jwt).await, Ok(()));
}

#[tokio::test(flavor = "multi_thread")]
async fn jwt_stops_when_the_source_pat_expires_inside_the_leeway() {
    let (a, b) = replicas().await;
    let (p, _, _) = pat(&a, "carol", &[], Some(Duration::from_secs(2))).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    writer(&a).await;
    assert_eq!(resolve(&a.url, &jwt).await, Ok(()));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(resolve(&a.url, &jwt).await, Err(Code::Unauthenticated));
    assert_eq!(resolve(&b.url, &jwt).await, Err(Code::Unauthenticated));
}

#[tokio::test(flavor = "multi_thread")]
async fn exchanged_expiry_is_never_after_the_source_expiry() {
    let (a, _b) = replicas().await;
    let (p, _, src_exp) = pat(&a, "carol", &[], Some(Duration::from_secs(60))).await;
    let (_, exp, _) = a.guard.exchange(&p).await.expect("exchange");
    assert!(exp <= src_exp.expect("source expiry"), "{exp} > {src_exp:?}");
    // A source without an expiry keeps the default lifetime.
    let (p2, _, _) = pat(&a, "carol", &[], None).await;
    let (_, exp2, _) = a.guard.exchange(&p2).await.expect("exchange");
    assert!(exp2 > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_sourced_jwt_stops_when_the_refresh_record_is_revoked() {
    let (a, b) = replicas().await;
    let (rt, rec) = a
        .guard
        .create_token("refresh", "carol", vec![], "scn login", Some(Duration::from_secs(3600)))
        .await
        .expect("refresh");
    let (jwt, _, _) = a.guard.exchange(&rt).await.expect("exchange");
    writer(&a).await;
    assert_eq!(resolve(&b.url, &jwt).await, Ok(()));
    assert!(a.guard.revoke_token(&rec.id).await.expect("revoke"));
    assert_eq!(resolve(&a.url, &jwt).await, Err(Code::Unauthenticated));
    assert_eq!(resolve(&b.url, &jwt).await, Err(Code::Unauthenticated));
}

#[tokio::test(flavor = "multi_thread")]
async fn external_jwt_exchange_is_unchanged() {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let (a, b) = replicas().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let ext = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"uuid": "u-1", "current_user": "carol", "iss": "OpenCSG", "exp": now + 3600}),
        &EncodingKey::from_secret(b"signing-key"),
    )
    .expect("sign");
    let (jwt, _, _) = a.guard.exchange(&ext).await.expect("exchange");
    writer(&a).await;
    assert_eq!(resolve(&a.url, &jwt).await, Ok(()));
    assert_eq!(resolve(&b.url, &jwt).await, Ok(()));
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_stream_ends_at_the_next_batch_after_source_revoke() {
    let (a, b) = replicas().await;
    let w = writer(&a).await;
    let (p, id, _) = pat(&a, "carol", &[], None).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    let mut stream = open_watch(&b.url, &jwt).await;
    assert!(a.guard.revoke_token(&id).await.expect("revoke"));
    land_change(&w, 2).await;
    let got = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("answered within 3 s");
    match got {
        Err(st) => assert_eq!(st.code(), Code::Unauthenticated, "{st}"),
        Ok(m) => panic!("stream delivered {m:?} after the source was revoked"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_stream_ends_at_the_next_batch_after_source_expiry() {
    let (a, _b) = replicas().await;
    let w = writer(&a).await;
    let (p, _, _) = pat(&a, "carol", &[], Some(Duration::from_secs(2))).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    let mut stream = open_watch(&a.url, &jwt).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    land_change(&w, 2).await;
    let got = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("answered within 3 s");
    match got {
        Err(st) => assert_eq!(st.code(), Code::Unauthenticated, "{st}"),
        Ok(m) => panic!("stream delivered {m:?} after the source expired"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_watch_stream_still_gets_the_event() {
    let (a, b) = replicas().await;
    let w = writer(&a).await;
    let (p, _, _) = pat(&a, "carol", &[], None).await;
    let (jwt, _, _) = a.guard.exchange(&p).await.expect("exchange");
    let mut stream = open_watch(&b.url, &jwt).await;
    land_change(&w, 2).await;
    let ev = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("timely")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.events[0].version, 2);
}
