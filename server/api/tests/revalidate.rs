//! An open WatchRefs stream must stop when its credential is revoked or expires, or when it loses read access.
//! Each test: (1) read the snapshot, (2) lose the credential, (3) a new request is refused,
//! (4) land a change and expect the stream to end with an error, not an event.

use std::sync::Arc;
use std::time::Duration;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client};
use scion_proto::v1::admin_service_client::AdminServiceClient;
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{ResolveRefRequest, RevokeTokenRequest, SetPolicyRequest, WatchRefsRequest, WatchRefsResponse};
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

[[rule]]
subjects = ["user:root"]
repos = ["*"]
actions = ["admin"]
"#;

const AUTH: &str = r#"
[token]
issuer = "https://scion.test"

[[issuer]]
issuer = "OpenCSG"
hs256_secret = "signing-key"
subject_claim = "current_user"
"#;

struct Server {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start() -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(AUTH).expect("auth config"),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            ref_poll_interval: Duration::from_millis(50),
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server {
        url,
        guard,
        _stop: tx,
    }
}

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

fn external_jwt(user: &str, exp_in_secs: u64) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"uuid": "u-1", "current_user": user, "iss": "OpenCSG", "exp": now + exp_in_secs}),
        &EncodingKey::from_secret(b"signing-key"),
    )
    .expect("sign")
}

async fn pat(s: &Server, subject: &str, groups: &[&str], ttl: Option<Duration>) -> (String, String) {
    let (t, rec) = s
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
    (t, rec.id)
}

async fn client(s: &Server, token: &str) -> Client {
    Client::builder()
        .endpoint(&s.url)
        .repo("mono")
        .auth(Auth::Bearer(token.into()))
        .build()
        .await
        .expect("connect")
}

/// Land one more change on main as `writer`.
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

async fn open_watch(s: &Server, token: &str) -> (RefServiceClient<tonic::transport::Channel>, Streaming<WatchRefsResponse>) {
    let mut refs = RefServiceClient::connect(s.url.clone()).await.expect("connect");
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
    let snap = stream.message().await.expect("snapshot").expect("msg");
    assert!(snap.snapshot, "step 1: snapshot first");
    (refs, stream)
}

async fn resolve_code(refs: &mut RefServiceClient<tonic::transport::Channel>, token: &str) -> Code {
    refs.resolve_ref(with_token(
        ResolveRefRequest {
            repo: "mono".into(),
            r#ref: "main".into(),
        },
        token,
    ))
    .await
    .expect_err("step 3: new request refused")
    .code()
}

/// Step 4: land a change; the stream must end with `want`, never deliver an event.
async fn expect_stream_error(stream: &mut Streaming<WatchRefsResponse>, writer: &Client, want: Code) {
    land_change(writer, 2).await;
    let got = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("stream answered within 3 s");
    match got {
        Err(st) => assert_eq!(st.code(), want, "{st}"),
        Ok(m) => panic!("stream delivered {m:?} after the credential was lost"),
    }
}

async fn writer_with_first_commit(s: &Server) -> Client {
    let (eng, _) = pat(s, "bob", &["eng"], None).await;
    let w = client(s, &eng).await;
    land_change(&w, 1).await;
    w
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_stream_still_gets_events() {
    let s = start().await;
    let w = writer_with_first_commit(&s).await;
    let (watcher, _) = pat(&s, "alice", &["eng"], None).await;
    let (_refs, mut stream) = open_watch(&s, &watcher).await;
    land_change(&w, 2).await;
    let ev = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("timely")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.events[0].version, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_ends_after_pat_revocation() {
    let s = start().await;
    let w = writer_with_first_commit(&s).await;
    let (watcher, id) = pat(&s, "alice", &["eng"], None).await;
    let (mut refs, mut stream) = open_watch(&s, &watcher).await;
    let (root, _) = pat(&s, "root", &[], None).await;
    let revoked = AdminServiceClient::connect(s.url.clone())
        .await
        .expect("admin")
        .revoke_token(with_token(RevokeTokenRequest { id }, &root))
        .await
        .expect("revoke")
        .into_inner();
    assert!(revoked.revoked);
    assert_eq!(resolve_code(&mut refs, &watcher).await, Code::Unauthenticated);
    expect_stream_error(&mut stream, &w, Code::Unauthenticated).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_ends_after_pat_expiry() {
    let s = start().await;
    let w = writer_with_first_commit(&s).await;
    let (watcher, _) = pat(&s, "alice", &["eng"], Some(Duration::from_secs(2))).await;
    let (mut refs, mut stream) = open_watch(&s, &watcher).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(resolve_code(&mut refs, &watcher).await, Code::Unauthenticated);
    expect_stream_error(&mut stream, &w, Code::Unauthenticated).await;
}

/// The external JWT verifier allows 30 s of clock leeway, so the token is refused 32 s after issue.
#[tokio::test(flavor = "multi_thread")]
async fn stream_ends_after_external_jwt_expiry() {
    let s = start().await;
    let w = writer_with_first_commit(&s).await;
    // The JWT carries no groups: the policy grants user:carol read on mono.
    let jwt = external_jwt("carol", 2);
    let (mut refs, mut stream) = open_watch(&s, &jwt).await;
    tokio::time::sleep(Duration::from_secs(33)).await;
    assert_eq!(resolve_code(&mut refs, &jwt).await, Code::Unauthenticated);
    expect_stream_error(&mut stream, &w, Code::Unauthenticated).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_ends_after_read_access_removed() {
    let s = start().await;
    let w = writer_with_first_commit(&s).await;
    let (watcher, _) = pat(&s, "alice", &["eng"], None).await;
    let (mut refs, mut stream) = open_watch(&s, &watcher).await;
    let (root, _) = pat(&s, "root", &[], None).await;
    AdminServiceClient::connect(s.url.clone())
        .await
        .expect("admin")
        .set_policy(with_token(
            SetPolicyRequest {
                policy: "[[rule]]\nsubjects = [\"group:eng\"]\nrepos = [\"other\"]\nactions = [\"read\"]\n\n\
                         [[rule]]\nsubjects = [\"user:bob\"]\nrepos = [\"mono\"]\nactions = [\"land\"]\n\n\
                         [[rule]]\nsubjects = [\"user:root\"]\nrepos = [\"*\"]\nactions = [\"admin\"]\n"
                    .into(),
            },
            &root,
        ))
        .await
        .expect("set policy");
    assert_eq!(resolve_code(&mut refs, &watcher).await, Code::PermissionDenied);
    expect_stream_error(&mut stream, &w, Code::PermissionDenied).await;
}
